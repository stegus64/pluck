using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Pluck.Config;
using Pluck.Source;
using Pluck.Util;
using System.Data;
using System.Globalization;
using CsvHelper;

namespace Pluck.Target;

public sealed record SqlServerDestinationChunkMetrics(TimeSpan BulkCopyElapsed, TimeSpan MergeElapsed, int RowsCopied);
public sealed record SqlServerDestinationDeleteMetrics(TimeSpan BulkCopyElapsed, TimeSpan SoftDeleteElapsed, int AffectedRows, int SourceKeysCopied);

public sealed class SqlServerDestinationLoader
{
    private readonly SqlServerDestinationConnectionFactory _factory;
    private readonly ILogger _log;

    public SqlServerDestinationLoader(SqlServerDestinationConnectionFactory factory, ILogger? log = null)
    {
        _factory = factory;
        _log = log ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
    }

    public async Task<object?> GetMaxUpdateKeyAsync(string schema, string table, string updateKey, string? logPrefix = null)
    {
        await using var conn = await _factory.OpenAsync(logPrefix: logPrefix);
        var sql = $"SELECT MAX([{updateKey}]) FROM [{schema}].[{table}];";
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        LogSql("get max update key", cmd, logPrefix);
        var v = await cmd.ExecuteScalarAsync();
        return v is DBNull ? null : v;
    }

    public async Task<WarehouseStreamState?> GetStreamStateAsync(string streamName, string? logPrefix = null)
    {
        await EnsureStreamStateTableAsync(logPrefix);
        await using var conn = await _factory.OpenAsync(logPrefix: logPrefix);
        var sql = @"
SELECT [stream_name], [ct_last_sync_version]
FROM [dbo].[__pluck_stream_state]
WHERE [stream_name] = @streamName;
";
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue("@streamName", streamName);
        LogSql("get stream state", cmd, logPrefix);
        await using var rdr = await cmd.ExecuteReaderAsync();
        if (!await rdr.ReadAsync())
            return null;

        return new WarehouseStreamState(
            StreamName: rdr.GetString(0),
            ChangeTrackingVersion: rdr.IsDBNull(1) ? null : rdr.GetInt64(1));
    }

    public async Task UpsertStreamChangeTrackingVersionAsync(string streamName, long ctLastSyncVersion, string? logPrefix = null)
    {
        await EnsureStreamStateTableAsync(logPrefix);
        await using var conn = await _factory.OpenAsync(logPrefix: logPrefix);
        var sql = @"
MERGE [dbo].[__pluck_stream_state] AS t
USING (SELECT @streamName AS stream_name, @ctLastSyncVersion AS ct_last_sync_version) AS s
ON t.[stream_name] = s.[stream_name]
WHEN MATCHED THEN
    UPDATE SET
        t.[ct_last_sync_version] = s.[ct_last_sync_version],
        t.[updated_utc] = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT ([stream_name], [ct_last_sync_version], [updated_utc])
    VALUES (s.[stream_name], s.[ct_last_sync_version], SYSUTCDATETIME());
";
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue("@streamName", streamName);
        cmd.Parameters.AddWithValue("@ctLastSyncVersion", ctLastSyncVersion);
        LogSql("upsert stream change tracking version", cmd, logPrefix);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<SqlServerDestinationChunkMetrics> LoadAndMergeAsync(
        string targetSchema,
        string targetTable,
        string tempTable,
        List<SourceColumn> columns,
        List<string> primaryKey,
        IAsyncEnumerable<object?[]> rows,
        CleanupConfig cleanup,
        string? logPrefix = null)
    {
        await using var conn = await _factory.OpenAsync(logPrefix: logPrefix);

        var colDefs = string.Join(",\n", columns.Select(c =>
            $"[{c.Name}] {TypeMapper.SqlServerToSqlServerType(c.SqlServerTypeName)} NULL"));

        var createTempSql = $@"
CREATE TABLE [{targetSchema}].[{tempTable}] (
{colDefs}
);
";
        await ExecAsync(conn, createTempSql, "create temp table", logPrefix);

        var (bulkElapsed, rowsCopied) = await BulkCopyIntoTempTableAsync(conn, targetSchema, tempTable, columns, rows, "bulk copy upsert rows", logPrefix);

        var loadedRowCount = await GetTableRowCountAsync(conn, targetSchema, tempTable, logPrefix);
        if (loadedRowCount != rowsCopied)
        {
            throw new Exception(
                $"Chunk row count mismatch before merge for [{targetSchema}].[{tempTable}]. " +
                $"Source rows={rowsCopied}, temp table rows={loadedRowCount}.");
        }

        TimeSpan mergeElapsed = TimeSpan.Zero;
        if (rowsCopied > 0)
        {
            var mergeSql = MergeBuilder.BuildMergeSql(targetSchema, targetTable, tempTable, columns, primaryKey);
            mergeElapsed = await ExecAsync(conn, mergeSql, "merge into target", logPrefix);
        }

        if (cleanup.DropTempTables)
        {
            var dropSql = $@"DROP TABLE [{targetSchema}].[{tempTable}];";
            await ExecAsync(conn, dropSql, "drop temp table", logPrefix);
        }

        return new SqlServerDestinationChunkMetrics(bulkElapsed, mergeElapsed, rowsCopied);
    }

    public async Task<SqlServerDestinationChunkMetrics> LoadAndMergeFromCsvAsync(
        string targetSchema,
        string targetTable,
        string tempTable,
        List<SourceColumn> columns,
        List<string> primaryKey,
        string csvPath,
        int expectedRowCount,
        CleanupConfig cleanup,
        string? logPrefix = null)
    {
        await using var conn = await _factory.OpenAsync(logPrefix: logPrefix);

        var colDefs = string.Join(",\n", columns.Select(c =>
            $"[{c.Name}] {TypeMapper.SqlServerToSqlServerType(c.SqlServerTypeName)} NULL"));

        var createTempSql = $@"
CREATE TABLE [{targetSchema}].[{tempTable}] (
{colDefs}
);
";
        await ExecAsync(conn, createTempSql, "create temp table", logPrefix);

        var bulkElapsed = await BulkCopyCsvIntoTempTableAsync(
            conn,
            targetSchema,
            tempTable,
            csvPath,
            expectedRowCount,
            "bulk copy upsert rows from csv",
            logPrefix);

        var loadedRowCount = await GetTableRowCountAsync(conn, targetSchema, tempTable, logPrefix);
        if (loadedRowCount != expectedRowCount)
        {
            throw new Exception(
                $"Chunk row count mismatch before merge for [{targetSchema}].[{tempTable}]. " +
                $"Source rows={expectedRowCount}, temp table rows={loadedRowCount}.");
        }

        var mergeSql = MergeBuilder.BuildMergeSql(targetSchema, targetTable, tempTable, columns, primaryKey);
        var mergeElapsed = await ExecAsync(conn, mergeSql, "merge into target", logPrefix);

        if (cleanup.DropTempTables)
        {
            var dropSql = $@"DROP TABLE [{targetSchema}].[{tempTable}];";
            await ExecAsync(conn, dropSql, "drop temp table", logPrefix);
        }

        return new SqlServerDestinationChunkMetrics(bulkElapsed, mergeElapsed, loadedRowCount);
    }

    public async Task<SqlServerDestinationDeleteMetrics> SoftDeleteMissingRowsAsync(
        string targetSchema,
        string targetTable,
        string sourceKeysTempTable,
        List<SourceColumn> primaryKeyColumns,
        IAsyncEnumerable<object?[]> sourceKeys,
        string? subsetWhere,
        CleanupConfig cleanup,
        string? logPrefix = null)
    {
        await using var conn = await _factory.OpenAsync(logPrefix: logPrefix);

        var pkDefs = string.Join(",\n", primaryKeyColumns.Select(c =>
            $"[{c.Name}] {TypeMapper.SqlServerToSqlServerType(c.SqlServerTypeName)} NULL"));
        var createTempSql = $@"
CREATE TABLE [{targetSchema}].[{sourceKeysTempTable}] (
{pkDefs}
);
";
        await ExecAsync(conn, createTempSql, "create source-keys temp table", logPrefix);

        var (bulkElapsed, sourceKeysCopied) = await BulkCopyIntoTempTableAsync(
            conn,
            targetSchema,
            sourceKeysTempTable,
            primaryKeyColumns,
            sourceKeys,
            "bulk copy source keys",
            logPrefix);

        var loadedSourceKeyRowCount = await GetTableRowCountAsync(conn, targetSchema, sourceKeysTempTable, logPrefix);
        if (loadedSourceKeyRowCount != sourceKeysCopied)
        {
            throw new Exception(
                $"Delete-detection key row count mismatch for [{targetSchema}].[{sourceKeysTempTable}]. " +
                $"Source keys={sourceKeysCopied}, temp table rows={loadedSourceKeyRowCount}.");
        }

        var pkJoin = string.Join(" AND ", primaryKeyColumns.Select(c => $"t.[{c.Name}] = s.[{c.Name}]"));
        var subsetPredicate = string.IsNullOrWhiteSpace(subsetWhere) ? "1=1" : $"({subsetWhere})";
        var softDeleteSql = $@"
UPDATE t
SET t.[_pluck_update_datetime] = SYSUTCDATETIME(),
    t.[_pluck_update_op] = 'D'
FROM [{targetSchema}].[{targetTable}] AS t
WHERE {subsetPredicate}
  AND NOT EXISTS (
      SELECT 1
      FROM [{targetSchema}].[{sourceKeysTempTable}] AS s
      WHERE {pkJoin}
  )
  AND ISNULL(t.[_pluck_update_op], '') <> 'D';
";
        var (softDeleteElapsed, affectedRows) = await ExecWithRowsAsync(conn, softDeleteSql, "soft delete missing rows", logPrefix);

        if (cleanup.DropTempTables)
        {
            var dropSql = $@"DROP TABLE [{targetSchema}].[{sourceKeysTempTable}];";
            await ExecAsync(conn, dropSql, "drop source-keys temp table", logPrefix);
        }

        return new SqlServerDestinationDeleteMetrics(bulkElapsed, softDeleteElapsed, affectedRows, loadedSourceKeyRowCount);
    }

    public async Task<SqlServerDestinationDeleteMetrics> SoftDeleteByKeysAsync(
        string targetSchema,
        string targetTable,
        string sourceKeysTempTable,
        List<SourceColumn> primaryKeyColumns,
        IAsyncEnumerable<object?[]> sourceKeys,
        CleanupConfig cleanup,
        string? logPrefix = null)
    {
        await using var conn = await _factory.OpenAsync(logPrefix: logPrefix);

        var pkDefs = string.Join(",\n", primaryKeyColumns.Select(c =>
            $"[{c.Name}] {TypeMapper.SqlServerToSqlServerType(c.SqlServerTypeName)} NULL"));
        var createTempSql = $@"
CREATE TABLE [{targetSchema}].[{sourceKeysTempTable}] (
{pkDefs}
);
";
        await ExecAsync(conn, createTempSql, "create ct-delete-keys temp table", logPrefix);

        var (bulkElapsed, sourceKeysCopied) = await BulkCopyIntoTempTableAsync(
            conn,
            targetSchema,
            sourceKeysTempTable,
            primaryKeyColumns,
            sourceKeys,
            "bulk copy ct-delete keys",
            logPrefix);

        var loadedSourceKeyRowCount = await GetTableRowCountAsync(conn, targetSchema, sourceKeysTempTable, logPrefix);
        if (loadedSourceKeyRowCount != sourceKeysCopied)
        {
            throw new Exception(
                $"CT delete-key row count mismatch for [{targetSchema}].[{sourceKeysTempTable}]. " +
                $"Source keys={sourceKeysCopied}, temp table rows={loadedSourceKeyRowCount}.");
        }

        var pkJoin = string.Join(" AND ", primaryKeyColumns.Select(c => $"t.[{c.Name}] = s.[{c.Name}]"));
        var softDeleteSql = $@"
UPDATE t
SET t.[_pluck_update_datetime] = SYSUTCDATETIME(),
    t.[_pluck_update_op] = 'D'
FROM [{targetSchema}].[{targetTable}] AS t
INNER JOIN [{targetSchema}].[{sourceKeysTempTable}] AS s
    ON {pkJoin}
WHERE ISNULL(t.[_pluck_update_op], '') <> 'D';
";
        var (softDeleteElapsed, affectedRows) = await ExecWithRowsAsync(conn, softDeleteSql, "soft delete ct rows", logPrefix);

        if (cleanup.DropTempTables)
        {
            var dropSql = $@"DROP TABLE [{targetSchema}].[{sourceKeysTempTable}];";
            await ExecAsync(conn, dropSql, "drop ct-delete-keys temp table", logPrefix);
        }

        return new SqlServerDestinationDeleteMetrics(bulkElapsed, softDeleteElapsed, affectedRows, loadedSourceKeyRowCount);
    }

    private async Task<(TimeSpan Elapsed, int RowsCopied)> BulkCopyIntoTempTableAsync(
        SqlConnection conn,
        string schema,
        string table,
        List<SourceColumn> columns,
        IAsyncEnumerable<object?[]> rows,
        string operation,
        string? logPrefix)
    {
        var method = (_factory.BulkLoad.Method ?? "SqlBulkCopy").Trim();
        if (!method.Equals("SqlBulkCopy", StringComparison.OrdinalIgnoreCase))
            throw new Exception($"Unsupported sqlServer.bulkLoad.method '{method}'. Supported values: SqlBulkCopy.");
        using var rowReader = new AsyncEnumerableObjectDataReader(columns, rows);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var bulkCopy = new SqlBulkCopy(conn)
        {
            DestinationTableName = $"[{schema}].[{table}]",
            BatchSize = _factory.BulkLoad.BatchSize > 0 ? _factory.BulkLoad.BatchSize : 10000,
            BulkCopyTimeout = _factory.BulkLoad.BulkCopyTimeoutSeconds >= 0 ? _factory.BulkLoad.BulkCopyTimeoutSeconds : 0
        };
        _log.LogDebug(
            "{LogPrefix}SQL BulkCopy ({Operation}): destination={DestinationTable}; rows={RowCount}; batchSize={BatchSize}; timeoutSec={TimeoutSec}",
            Prefix(logPrefix),
            operation,
            bulkCopy.DestinationTableName,
            "<stream>",
            bulkCopy.BatchSize,
            bulkCopy.BulkCopyTimeout);
        await bulkCopy.WriteToServerAsync(rowReader);
        sw.Stop();
        _log.LogDebug(
            "{LogPrefix}{Operation} elapsed: {Elapsed}ms; rowsCopied={RowsCopied}",
            Prefix(logPrefix),
            operation,
            sw.Elapsed.TotalMilliseconds,
            rowReader.RowsRead);
        return (sw.Elapsed, rowReader.RowsRead);
    }

    private async Task<TimeSpan> BulkCopyCsvIntoTempTableAsync(
        SqlConnection conn,
        string schema,
        string table,
        string csvPath,
        int expectedRowCount,
        string operation,
        string? logPrefix)
    {
        var method = (_factory.BulkLoad.Method ?? "SqlBulkCopy").Trim();
        if (!method.Equals("SqlBulkCopy", StringComparison.OrdinalIgnoreCase))
            throw new Exception($"Unsupported sqlServer.bulkLoad.method '{method}'. Supported values: SqlBulkCopy.");

        using var sr = new StreamReader(csvPath);
        using var csv = new CsvReader(sr, CultureInfo.InvariantCulture);
        using var csvDataReader = new CsvDataReader(csv);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var bulkCopy = new SqlBulkCopy(conn)
        {
            DestinationTableName = $"[{schema}].[{table}]",
            BatchSize = _factory.BulkLoad.BatchSize > 0 ? _factory.BulkLoad.BatchSize : 10000,
            BulkCopyTimeout = _factory.BulkLoad.BulkCopyTimeoutSeconds >= 0 ? _factory.BulkLoad.BulkCopyTimeoutSeconds : 0
        };
        _log.LogDebug(
            "{LogPrefix}SQL BulkCopy ({Operation}): destination={DestinationTable}; csvPath={CsvPath}; expectedRows={ExpectedRows}; batchSize={BatchSize}; timeoutSec={TimeoutSec}",
            Prefix(logPrefix),
            operation,
            bulkCopy.DestinationTableName,
            csvPath,
            expectedRowCount,
            bulkCopy.BatchSize,
            bulkCopy.BulkCopyTimeout);
        await bulkCopy.WriteToServerAsync(csvDataReader);
        sw.Stop();
        _log.LogDebug("{LogPrefix}{Operation} elapsed: {Elapsed}ms", Prefix(logPrefix), operation, sw.Elapsed.TotalMilliseconds);
        return sw.Elapsed;
    }

    private async Task<TimeSpan> ExecAsync(SqlConnection conn, string sql, string operation, string? logPrefix = null)
    {
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        LogSql(operation, cmd, logPrefix);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await cmd.ExecuteNonQueryAsync();
        sw.Stop();
        return sw.Elapsed;
    }

    private async Task<(TimeSpan Elapsed, int Rows)> ExecWithRowsAsync(SqlConnection conn, string sql, string operation, string? logPrefix = null)
    {
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        LogSql(operation, cmd, logPrefix);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var rows = await cmd.ExecuteNonQueryAsync();
        sw.Stop();
        return (sw.Elapsed, rows);
    }

    private async Task<int> GetTableRowCountAsync(SqlConnection conn, string schema, string table, string? logPrefix)
    {
        var sql = $@"SELECT COUNT(*) FROM [{schema}].[{table}];";
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        LogSql("get table row count", cmd, logPrefix);
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    private async Task EnsureStreamStateTableAsync(string? logPrefix = null)
    {
        await using var conn = await _factory.OpenAsync(logPrefix: logPrefix);
        var sql = @"
IF OBJECT_ID(N'dbo.__pluck_stream_state', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[__pluck_stream_state] (
        [stream_name] nvarchar(256) NOT NULL,
        [ct_last_sync_version] bigint NULL,
        [updated_utc] datetime2(0) NOT NULL,
        CONSTRAINT [PK___pluck_stream_state] PRIMARY KEY ([stream_name])
    );
END;
";
        await ExecAsync(conn, sql, "ensure stream state table", logPrefix);
    }

    private static string Prefix(string? logPrefix) =>
        string.IsNullOrWhiteSpace(logPrefix) ? "[app] " : $"{logPrefix} ";

    private void LogSql(string operation, SqlCommand cmd, string? logPrefix)
    {
        _log.LogDebug(
            "{LogPrefix}SQL ({Operation}): {SqlCompact}",
            Prefix(logPrefix),
            operation,
            CompactSql(cmd.CommandText));
        _log.LogTrace(
            "{LogPrefix}SQL ({Operation}) full text: {Sql}",
            Prefix(logPrefix),
            operation,
            cmd.CommandText);
        _log.LogTrace(
            "{LogPrefix}SQL ({Operation}) params: {Params}",
            Prefix(logPrefix),
            operation,
            SqlLogFormatter.FormatParameters(cmd.Parameters));
    }

    private static string CompactSql(string sql)
    {
        var compact = string.Join(" ", sql.Split(['\r', '\n', '\t'], StringSplitOptions.RemoveEmptyEntries)).Trim();
        const int max = 1200;
        return compact.Length <= max ? compact : compact[..max] + "...";
    }
}

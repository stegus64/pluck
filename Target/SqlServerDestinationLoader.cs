using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Pluck.Config;
using Pluck.Source;
using Pluck.Util;
using System.Data;

namespace Pluck.Target;

public sealed record SqlServerDestinationChunkMetrics(TimeSpan BulkCopyElapsed, TimeSpan MergeElapsed);
public sealed record SqlServerDestinationDeleteMetrics(TimeSpan BulkCopyElapsed, TimeSpan SoftDeleteElapsed, int AffectedRows);

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
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<SqlServerDestinationChunkMetrics> LoadAndMergeAsync(
        string targetSchema,
        string targetTable,
        string tempTable,
        List<SourceColumn> columns,
        List<string> primaryKey,
        List<object?[]> rows,
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

        var bulkElapsed = await BulkCopyIntoTempTableAsync(conn, targetSchema, tempTable, columns, rows, "bulk copy upsert rows", logPrefix);

        var loadedRowCount = await GetTableRowCountAsync(conn, targetSchema, tempTable);
        if (loadedRowCount != rows.Count)
        {
            throw new Exception(
                $"Chunk row count mismatch before merge for [{targetSchema}].[{tempTable}]. " +
                $"Source rows={rows.Count}, temp table rows={loadedRowCount}.");
        }

        var mergeSql = MergeBuilder.BuildMergeSql(targetSchema, targetTable, tempTable, columns, primaryKey);
        var mergeElapsed = await ExecAsync(conn, mergeSql, "merge into target", logPrefix);

        if (cleanup.DropTempTables)
        {
            var dropSql = $@"DROP TABLE [{targetSchema}].[{tempTable}];";
            await ExecAsync(conn, dropSql, "drop temp table", logPrefix);
        }

        return new SqlServerDestinationChunkMetrics(bulkElapsed, mergeElapsed);
    }

    public async Task<SqlServerDestinationDeleteMetrics> SoftDeleteMissingRowsAsync(
        string targetSchema,
        string targetTable,
        string sourceKeysTempTable,
        List<SourceColumn> primaryKeyColumns,
        List<object?[]> sourceKeys,
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

        var bulkElapsed = await BulkCopyIntoTempTableAsync(conn, targetSchema, sourceKeysTempTable, primaryKeyColumns, sourceKeys, "bulk copy source keys", logPrefix);

        var loadedSourceKeyRowCount = await GetTableRowCountAsync(conn, targetSchema, sourceKeysTempTable);
        if (loadedSourceKeyRowCount != sourceKeys.Count)
        {
            throw new Exception(
                $"Delete-detection key row count mismatch for [{targetSchema}].[{sourceKeysTempTable}]. " +
                $"Source keys={sourceKeys.Count}, temp table rows={loadedSourceKeyRowCount}.");
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

        return new SqlServerDestinationDeleteMetrics(bulkElapsed, softDeleteElapsed, affectedRows);
    }

    public async Task<SqlServerDestinationDeleteMetrics> SoftDeleteByKeysAsync(
        string targetSchema,
        string targetTable,
        string sourceKeysTempTable,
        List<SourceColumn> primaryKeyColumns,
        List<object?[]> sourceKeys,
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

        var bulkElapsed = await BulkCopyIntoTempTableAsync(conn, targetSchema, sourceKeysTempTable, primaryKeyColumns, sourceKeys, "bulk copy ct-delete keys", logPrefix);

        var loadedSourceKeyRowCount = await GetTableRowCountAsync(conn, targetSchema, sourceKeysTempTable);
        if (loadedSourceKeyRowCount != sourceKeys.Count)
        {
            throw new Exception(
                $"CT delete-key row count mismatch for [{targetSchema}].[{sourceKeysTempTable}]. " +
                $"Source keys={sourceKeys.Count}, temp table rows={loadedSourceKeyRowCount}.");
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

        return new SqlServerDestinationDeleteMetrics(bulkElapsed, softDeleteElapsed, affectedRows);
    }

    private async Task<TimeSpan> BulkCopyIntoTempTableAsync(
        SqlConnection conn,
        string schema,
        string table,
        List<SourceColumn> columns,
        List<object?[]> rows,
        string operation,
        string? logPrefix)
    {
        if (rows.Count == 0)
            return TimeSpan.Zero;

        var method = (_factory.BulkLoad.Method ?? "SqlBulkCopy").Trim();
        if (!method.Equals("SqlBulkCopy", StringComparison.OrdinalIgnoreCase))
            throw new Exception($"Unsupported sqlServer.bulkLoad.method '{method}'. Supported values: SqlBulkCopy.");

        var dataTable = BuildDataTable(columns, rows);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var bulkCopy = new SqlBulkCopy(conn)
        {
            DestinationTableName = $"[{schema}].[{table}]",
            BatchSize = _factory.BulkLoad.BatchSize > 0 ? _factory.BulkLoad.BatchSize : 10000,
            BulkCopyTimeout = _factory.BulkLoad.BulkCopyTimeoutSeconds >= 0 ? _factory.BulkLoad.BulkCopyTimeoutSeconds : 0
        };
        foreach (var c in columns)
            bulkCopy.ColumnMappings.Add(c.Name, c.Name);
        await bulkCopy.WriteToServerAsync(dataTable);
        sw.Stop();
        _log.LogDebug("{LogPrefix}{Operation} elapsed: {Elapsed}ms", Prefix(logPrefix), operation, sw.Elapsed.TotalMilliseconds);
        return sw.Elapsed;
    }

    private static DataTable BuildDataTable(List<SourceColumn> columns, List<object?[]> rows)
    {
        var table = new DataTable();
        foreach (var c in columns)
            table.Columns.Add(c.Name, typeof(object));

        foreach (var row in rows)
        {
            var dr = table.NewRow();
            for (var i = 0; i < columns.Count; i++)
                dr[i] = row[i] ?? DBNull.Value;
            table.Rows.Add(dr);
        }

        return table;
    }

    private async Task<TimeSpan> ExecAsync(SqlConnection conn, string sql, string operation, string? logPrefix = null)
    {
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        _log.LogDebug("{LogPrefix}SQL Exec ({Operation}) started.", Prefix(logPrefix), operation);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await cmd.ExecuteNonQueryAsync();
        sw.Stop();
        return sw.Elapsed;
    }

    private async Task<(TimeSpan Elapsed, int Rows)> ExecWithRowsAsync(SqlConnection conn, string sql, string operation, string? logPrefix = null)
    {
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        _log.LogDebug("{LogPrefix}SQL ExecWithRows ({Operation}) started.", Prefix(logPrefix), operation);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var rows = await cmd.ExecuteNonQueryAsync();
        sw.Stop();
        return (sw.Elapsed, rows);
    }

    private async Task<int> GetTableRowCountAsync(SqlConnection conn, string schema, string table)
    {
        var sql = $@"SELECT COUNT(*) FROM [{schema}].[{table}];";
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
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
}

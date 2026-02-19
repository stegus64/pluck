using Pluck.Config;
using Pluck.Source;
using Pluck.Util;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Pluck.Target;

public sealed record WarehouseChunkMetrics(TimeSpan CopyIntoElapsed, TimeSpan MergeElapsed);
public sealed record WarehouseDeleteMetrics(TimeSpan CopyIntoElapsed, TimeSpan SoftDeleteElapsed, int AffectedRows);
public sealed record WarehouseStreamState(string StreamName, long? ChangeTrackingVersion);

public sealed class WarehouseLoader
{
    private readonly WarehouseConnectionFactory _factory;
    private readonly Microsoft.Extensions.Logging.ILogger _log;
    public WarehouseLoader(WarehouseConnectionFactory factory, Microsoft.Extensions.Logging.ILogger? log = null)
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
        _log.LogDebug("{LogPrefix}GetMaxUpdateKey execution started.", Prefix(logPrefix));
        _log.LogTrace("{LogPrefix}SQL GetMaxUpdateKey: {Sql}", Prefix(logPrefix), sql);
        var sw0 = System.Diagnostics.Stopwatch.StartNew();
        var v = await cmd.ExecuteScalarAsync();
        sw0.Stop();
        _log.LogDebug("{LogPrefix}GetMaxUpdateKey elapsed: {Elapsed}ms", Prefix(logPrefix), sw0.Elapsed.TotalMilliseconds);
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
        _log.LogDebug("{LogPrefix}GetStreamState execution started.", Prefix(logPrefix));
        _log.LogTrace("{LogPrefix}SQL GetStreamState: {Sql}", Prefix(logPrefix), sql);
        _log.LogTrace("{LogPrefix}SQL GetStreamState Params: {Params}", Prefix(logPrefix), SqlLogFormatter.FormatParameters(cmd.Parameters));

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
        _log.LogDebug("{LogPrefix}UpsertStreamChangeTrackingVersion execution started.", Prefix(logPrefix));
        _log.LogTrace("{LogPrefix}SQL UpsertStreamChangeTrackingVersion: {Sql}", Prefix(logPrefix), sql);
        _log.LogTrace("{LogPrefix}SQL UpsertStreamChangeTrackingVersion Params: {Params}", Prefix(logPrefix), SqlLogFormatter.FormatParameters(cmd.Parameters));
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<WarehouseChunkMetrics> LoadAndMergeAsync(
        string targetSchema,
        string targetTable,
        string tempTable,
        List<SourceColumn> columns,
        List<string> primaryKey,
        int expectedRowCount,
        string oneLakeDfsUrl,
        string stagingFileFormat,
        CleanupConfig cleanup,
        string? logPrefix = null)
    {
        await using var conn = await _factory.OpenAsync(logPrefix: logPrefix);

        // 1) Create temp table
        var colDefs = string.Join(",\n", columns.Select(c =>
            $"[{c.Name}] {TypeMapper.SqlServerToFabricWarehouseType(c.SqlServerTypeName)} NULL"));

        var createTempSql = $@"
CREATE TABLE [{targetSchema}].[{tempTable}] (
{colDefs}
);
";
        await ExecAsync(conn, createTempSql, "create temp table", logPrefix);

        // 2) COPY INTO from OneLake (CSV + gzip supported; OneLake supported as source in Fabric) :contentReference[oaicite:8]{index=8}
        var colList = string.Join(",", columns.Select(c => $"[{c.Name}]"));
        var copySql = BuildCopyIntoSql(targetSchema, tempTable, colList, oneLakeDfsUrl, stagingFileFormat);
        var copyIntoElapsed = await ExecAsync(conn, copySql, "copy into temp table", logPrefix);

        // 3) Verify rows loaded into temp table match source chunk rows
        var loadedRowCount = await GetTableRowCountAsync(conn, targetSchema, tempTable);
        if (loadedRowCount != expectedRowCount)
        {
            throw new Exception(
                $"Chunk row count mismatch before merge for [{targetSchema}].[{tempTable}]. " +
                $"Source rows={expectedRowCount}, temp table rows={loadedRowCount}.");
        }

        _log.LogDebug(
            "{LogPrefix}Chunk row count verified for temp table [{Schema}].[{Table}]: {RowCount} rows.",
            Prefix(logPrefix),
            targetSchema,
            tempTable,
            loadedRowCount);

        // 4) MERGE into target (MERGE supported in Fabric Warehouse per official surface area) :contentReference[oaicite:9]{index=9}
        var mergeSql = MergeBuilder.BuildMergeSql(targetSchema, targetTable, tempTable, columns, primaryKey);
        var mergeElapsed = await ExecAsync(conn, mergeSql, "merge into target", logPrefix);

        // 5) Cleanup temp table
        if (cleanup.DropTempTables)
        {
            var dropSql = $@"DROP TABLE [{targetSchema}].[{tempTable}];";
            await ExecAsync(conn, dropSql, "drop temp table", logPrefix);
        }

        return new WarehouseChunkMetrics(copyIntoElapsed, mergeElapsed);
    }

    public async Task<WarehouseDeleteMetrics> SoftDeleteMissingRowsAsync(
        string targetSchema,
        string targetTable,
        string sourceKeysTempTable,
        List<SourceColumn> primaryKeyColumns,
        int expectedSourceKeyRowCount,
        string sourceKeysDfsUrl,
        string sourceKeysFileFormat,
        string? subsetWhere,
        CleanupConfig cleanup,
        string? logPrefix = null)
    {
        await using var conn = await _factory.OpenAsync(logPrefix: logPrefix);

        var pkDefs = string.Join(",\n", primaryKeyColumns.Select(c =>
            $"[{c.Name}] {TypeMapper.SqlServerToFabricWarehouseType(c.SqlServerTypeName)} NULL"));
        var createTempSql = $@"
CREATE TABLE [{targetSchema}].[{sourceKeysTempTable}] (
{pkDefs}
);
";
        await ExecAsync(conn, createTempSql, "create source-keys temp table", logPrefix);

        var pkColList = string.Join(",", primaryKeyColumns.Select(c => $"[{c.Name}]"));
        var copySql = BuildCopyIntoSql(targetSchema, sourceKeysTempTable, pkColList, sourceKeysDfsUrl, sourceKeysFileFormat);
        var copyElapsed = await ExecAsync(conn, copySql, "copy source keys into temp table", logPrefix);
        var loadedSourceKeyRowCount = await GetTableRowCountAsync(conn, targetSchema, sourceKeysTempTable);
        if (loadedSourceKeyRowCount != expectedSourceKeyRowCount)
        {
            throw new Exception(
                $"Delete-detection key row count mismatch for [{targetSchema}].[{sourceKeysTempTable}]. " +
                $"Source keys={expectedSourceKeyRowCount}, temp table rows={loadedSourceKeyRowCount}.");
        }
        _log.LogDebug(
            "{LogPrefix}Delete-detection key row count verified for temp table [{Schema}].[{Table}]: {RowCount} rows.",
            Prefix(logPrefix),
            targetSchema,
            sourceKeysTempTable,
            loadedSourceKeyRowCount);

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

        return new WarehouseDeleteMetrics(copyElapsed, softDeleteElapsed, affectedRows);
    }

    public async Task<WarehouseDeleteMetrics> SoftDeleteByKeysAsync(
        string targetSchema,
        string targetTable,
        string sourceKeysTempTable,
        List<SourceColumn> primaryKeyColumns,
        int expectedSourceKeyRowCount,
        string sourceKeysDfsUrl,
        string sourceKeysFileFormat,
        CleanupConfig cleanup,
        string? logPrefix = null)
    {
        await using var conn = await _factory.OpenAsync(logPrefix: logPrefix);

        var pkDefs = string.Join(",\n", primaryKeyColumns.Select(c =>
            $"[{c.Name}] {TypeMapper.SqlServerToFabricWarehouseType(c.SqlServerTypeName)} NULL"));
        var createTempSql = $@"
CREATE TABLE [{targetSchema}].[{sourceKeysTempTable}] (
{pkDefs}
);
";
        await ExecAsync(conn, createTempSql, "create ct-delete-keys temp table", logPrefix);

        var pkColList = string.Join(",", primaryKeyColumns.Select(c => $"[{c.Name}]"));
        var copySql = BuildCopyIntoSql(targetSchema, sourceKeysTempTable, pkColList, sourceKeysDfsUrl, sourceKeysFileFormat);
        var copyElapsed = await ExecAsync(conn, copySql, "copy ct-delete-keys into temp table", logPrefix);
        var loadedSourceKeyRowCount = await GetTableRowCountAsync(conn, targetSchema, sourceKeysTempTable);
        if (loadedSourceKeyRowCount != expectedSourceKeyRowCount)
        {
            throw new Exception(
                $"CT delete-key row count mismatch for [{targetSchema}].[{sourceKeysTempTable}]. " +
                $"Source keys={expectedSourceKeyRowCount}, temp table rows={loadedSourceKeyRowCount}.");
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

        return new WarehouseDeleteMetrics(copyElapsed, softDeleteElapsed, affectedRows);
    }

    private static string BuildCopyIntoSql(
        string targetSchema,
        string tempTable,
        string colList,
        string sourceUrl,
        string stagingFileFormat)
    {
        var f = (stagingFileFormat ?? "csv.gz").Trim().ToLowerInvariant();
        var isParquet = f is "parquet" or "pq";
        var isUncompressedCsv = f is "csv";

        var withClause = isParquet
            ? "FILE_TYPE = 'PARQUET'"
            : isUncompressedCsv
                ? "FILE_TYPE = 'CSV',\n    FIRSTROW = 2,\n    FIELDTERMINATOR = ',',\n    ROWTERMINATOR = '0x0A'"
                : "FILE_TYPE = 'CSV',\n    COMPRESSION = 'GZIP',\n    FIRSTROW = 2,\n    FIELDTERMINATOR = ',',\n    ROWTERMINATOR = '0x0A'";

        return $@"
COPY INTO [{targetSchema}].[{tempTable}] ({colList})
FROM '{sourceUrl}'
WITH (
    {withClause}
);
";
    }

    private async Task<TimeSpan> ExecAsync(SqlConnection conn, string sql, string operation, string? logPrefix = null)
    {
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        _log.LogDebug("{LogPrefix}SQL Exec ({Operation}) started.", Prefix(logPrefix), operation);
        _log.LogTrace("{LogPrefix}SQL Exec ({Operation}): {Sql}", Prefix(logPrefix), operation, sql);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await cmd.ExecuteNonQueryAsync();
            sw.Stop();
            _log.LogDebug("{LogPrefix}Exec elapsed: {Elapsed}ms", Prefix(logPrefix), sw.Elapsed.TotalMilliseconds);
            return sw.Elapsed;
        }
        catch (SqlException ex)
        {
            sw.Stop();
            _log.LogError(
                ex,
                "{LogPrefix}SQL operation failed ({Operation}) after {ElapsedMs:F0}ms. Number={ErrorNumber}, State={State}, Class={Class}, ClientConnectionId={ClientConnectionId}",
                Prefix(logPrefix),
                operation,
                sw.Elapsed.TotalMilliseconds,
                ex.Number,
                ex.State,
                ex.Class,
                ex.ClientConnectionId);
            _log.LogTrace("{LogPrefix}Failed SQL ({Operation}): {Sql}", Prefix(logPrefix), operation, CompactSql(sql));
            throw;
        }
    }

    private async Task<(TimeSpan Elapsed, int Rows)> ExecWithRowsAsync(SqlConnection conn, string sql, string operation, string? logPrefix = null)
    {
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        _log.LogDebug("{LogPrefix}SQL ExecWithRows ({Operation}) started.", Prefix(logPrefix), operation);
        _log.LogTrace("{LogPrefix}SQL ExecWithRows ({Operation}): {Sql}", Prefix(logPrefix), operation, sql);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            var rows = await cmd.ExecuteNonQueryAsync();
            sw.Stop();
            _log.LogDebug("{LogPrefix}Exec elapsed: {Elapsed}ms, rows={Rows}", Prefix(logPrefix), sw.Elapsed.TotalMilliseconds, rows);
            return (sw.Elapsed, rows);
        }
        catch (SqlException ex)
        {
            sw.Stop();
            _log.LogError(
                ex,
                "{LogPrefix}SQL operation failed ({Operation}) after {ElapsedMs:F0}ms. Number={ErrorNumber}, State={State}, Class={Class}, ClientConnectionId={ClientConnectionId}",
                Prefix(logPrefix),
                operation,
                sw.Elapsed.TotalMilliseconds,
                ex.Number,
                ex.State,
                ex.Class,
                ex.ClientConnectionId);
            _log.LogTrace("{LogPrefix}Failed SQL ({Operation}): {Sql}", Prefix(logPrefix), operation, CompactSql(sql));
            throw;
        }
    }

    private static string CompactSql(string sql)
    {
        var compact = string.Join(" ", sql.Split(['\r', '\n', '\t'], StringSplitOptions.RemoveEmptyEntries)).Trim();
        const int max = 1200;
        return compact.Length <= max ? compact : compact[..max] + "...";
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

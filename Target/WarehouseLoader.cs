using FabricIncrementalReplicator.Config;
using FabricIncrementalReplicator.Source;
using FabricIncrementalReplicator.Util;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace FabricIncrementalReplicator.Target;

public sealed record WarehouseChunkMetrics(TimeSpan CopyIntoElapsed, TimeSpan MergeElapsed);
public sealed record WarehouseDeleteMetrics(TimeSpan CopyIntoElapsed, TimeSpan SoftDeleteElapsed, int AffectedRows);

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

        var pkJoin = string.Join(" AND ", primaryKeyColumns.Select(c => $"t.[{c.Name}] = s.[{c.Name}]"));
        var subsetPredicate = string.IsNullOrWhiteSpace(subsetWhere) ? "1=1" : $"({subsetWhere})";
        var softDeleteSql = $@"
UPDATE t
SET t.[_sg_update_datetime] = SYSUTCDATETIME(),
    t.[_sg_update_op] = 'D'
FROM [{targetSchema}].[{targetTable}] AS t
WHERE {subsetPredicate}
  AND NOT EXISTS (
      SELECT 1
      FROM [{targetSchema}].[{sourceKeysTempTable}] AS s
      WHERE {pkJoin}
  )
  AND ISNULL(t.[_sg_update_op], '') <> 'D';
";
        var (softDeleteElapsed, affectedRows) = await ExecWithRowsAsync(conn, softDeleteSql, "soft delete missing rows", logPrefix);

        if (cleanup.DropTempTables)
        {
            var dropSql = $@"DROP TABLE [{targetSchema}].[{sourceKeysTempTable}];";
            await ExecAsync(conn, dropSql, "drop source-keys temp table", logPrefix);
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

    private static string Prefix(string? logPrefix) =>
        string.IsNullOrWhiteSpace(logPrefix) ? "[app] " : $"{logPrefix} ";
}

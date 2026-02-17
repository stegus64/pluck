using FabricIncrementalReplicator.Config;
using FabricIncrementalReplicator.Source;
using FabricIncrementalReplicator.Util;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace FabricIncrementalReplicator.Target;

public sealed record WarehouseChunkMetrics(TimeSpan CopyIntoElapsed, TimeSpan MergeElapsed);

public sealed class WarehouseLoader
{
    private readonly WarehouseConnectionFactory _factory;
    private readonly Microsoft.Extensions.Logging.ILogger<WarehouseLoader> _log;
    public WarehouseLoader(WarehouseConnectionFactory factory, Microsoft.Extensions.Logging.ILogger<WarehouseLoader>? log = null)
    {
        _factory = factory;
        _log = log ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<WarehouseLoader>.Instance;
    }

    public async Task<object?> GetMaxUpdateKeyAsync(string schema, string table, string updateKey)
    {
        await using var conn = await _factory.OpenAsync();
        var sql = $"SELECT MAX([{updateKey}]) FROM [{schema}].[{table}];";
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        _log.LogDebug("SQL GetMaxUpdateKey: {Sql}", sql);
        var sw0 = System.Diagnostics.Stopwatch.StartNew();
        var v = await cmd.ExecuteScalarAsync();
        sw0.Stop();
        _log.LogDebug("GetMaxUpdateKey elapsed: {Elapsed}ms", sw0.Elapsed.TotalMilliseconds);
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
        CleanupConfig cleanup)
    {
        await using var conn = await _factory.OpenAsync();

        // 1) Create temp table
        var colDefs = string.Join(",\n", columns.Select(c =>
            $"[{c.Name}] {TypeMapper.SqlServerToFabricWarehouseType(c.SqlServerTypeName)} NULL"));

        var createTempSql = $@"
CREATE TABLE [{targetSchema}].[{tempTable}] (
{colDefs}
);
";
        await ExecAsync(conn, createTempSql, "create temp table");

        // 2) COPY INTO from OneLake (CSV + gzip supported; OneLake supported as source in Fabric) :contentReference[oaicite:8]{index=8}
        var colList = string.Join(",", columns.Select(c => $"[{c.Name}]"));
        var copySql = BuildCopyIntoSql(targetSchema, tempTable, colList, oneLakeDfsUrl, stagingFileFormat);
        _log.LogDebug("COPY INTO SQL: {Sql}", copySql);
        var copyIntoElapsed = await ExecAsync(conn, copySql, "copy into temp table");

        // 3) Verify rows loaded into temp table match source chunk rows
        var loadedRowCount = await GetTableRowCountAsync(conn, targetSchema, tempTable);
        if (loadedRowCount != expectedRowCount)
        {
            throw new Exception(
                $"Chunk row count mismatch before merge for [{targetSchema}].[{tempTable}]. " +
                $"Source rows={expectedRowCount}, temp table rows={loadedRowCount}.");
        }

        _log.LogDebug(
            "Chunk row count verified for temp table [{Schema}].[{Table}]: {RowCount} rows.",
            targetSchema,
            tempTable,
            loadedRowCount);

        // 4) MERGE into target (MERGE supported in Fabric Warehouse per official surface area) :contentReference[oaicite:9]{index=9}
        var mergeSql = MergeBuilder.BuildMergeSql(targetSchema, targetTable, tempTable, columns, primaryKey);
        _log.LogDebug("MERGE SQL: {Sql}", mergeSql);
        var mergeElapsed = await ExecAsync(conn, mergeSql, "merge into target");

        // 5) Cleanup temp table
        if (cleanup.DropTempTables)
        {
            var dropSql = $@"DROP TABLE [{targetSchema}].[{tempTable}];";
            await ExecAsync(conn, dropSql, "drop temp table");
        }

        return new WarehouseChunkMetrics(copyIntoElapsed, mergeElapsed);
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

    private async Task<TimeSpan> ExecAsync(SqlConnection conn, string sql, string operation)
    {
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await cmd.ExecuteNonQueryAsync();
            sw.Stop();
            _log.LogDebug("Exec elapsed: {Elapsed}ms", sw.Elapsed.TotalMilliseconds);
            return sw.Elapsed;
        }
        catch (SqlException ex)
        {
            sw.Stop();
            _log.LogError(
                ex,
                "SQL operation failed ({Operation}) after {ElapsedMs:F0}ms. Number={ErrorNumber}, State={State}, Class={Class}, ClientConnectionId={ClientConnectionId}, Sql={Sql}",
                operation,
                sw.Elapsed.TotalMilliseconds,
                ex.Number,
                ex.State,
                ex.Class,
                ex.ClientConnectionId,
                CompactSql(sql));
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
}

using FabricIncrementalReplicator.Source;
using FabricIncrementalReplicator.Util;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace FabricIncrementalReplicator.Target;

public sealed class WarehouseSchemaManager
{
    private readonly WarehouseConnectionFactory _factory;
    private readonly Microsoft.Extensions.Logging.ILogger _log;
    public WarehouseSchemaManager(WarehouseConnectionFactory factory, Microsoft.Extensions.Logging.ILogger? log = null)
    {
        _factory = factory;
        _log = log ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
    }

    public async Task EnsureTableAndSchemaAsync(string schema, string table, List<SourceColumn> sourceColumns, List<string> primaryKey, string? logPrefix = null)
    {
        await using var conn = await _factory.OpenAsync(logPrefix: logPrefix);

        // Ensure schema exists
        var ensureSchemaSql = $@"
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = @schema)
    EXEC('CREATE SCHEMA [{schema}]');
";
        await ExecAsync(conn, ensureSchemaSql, logPrefix, ("@schema", schema));

        // Create table if not exists
        var colDefs = string.Join(",\n",
            sourceColumns.Select(c => $"[{c.Name}] {TypeMapper.SqlServerToFabricWarehouseType(c.SqlServerTypeName)} NULL"));

        var createSql = $@"
IF OBJECT_ID(@fullName, 'U') IS NULL
BEGIN
    EXEC('CREATE TABLE [{schema}].[{table}] (
{colDefs}
    )');
END
";
        await ExecAsync(conn, createSql, logPrefix, ("@fullName", $"{schema}.{table}"));

        // Add missing columns
        var targetCols = await GetTargetColumnsAsync(conn, schema, table, logPrefix);

        var missing = sourceColumns
            .Where(sc => !targetCols.Contains(sc.Name, StringComparer.OrdinalIgnoreCase))
            .ToList();

        foreach (var m in missing)
        {
            var addSql = $@"ALTER TABLE [{schema}].[{table}] ADD [{m.Name}] {TypeMapper.SqlServerToFabricWarehouseType(m.SqlServerTypeName)} NULL;";
            await ExecAsync(conn, addSql, logPrefix);
        }

        await EnsureMetadataColumnsAsync(conn, schema, table, logPrefix);
    }

    private async Task<HashSet<string>> GetTargetColumnsAsync(SqlConnection conn, string schema, string table, string? logPrefix = null)
    {
        var sql = @"
SELECT c.name
FROM sys.columns c
JOIN sys.tables t ON t.object_id = c.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = @schema AND t.name = @table;
";
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", table);

        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("{LogPrefix}GetTargetColumns execution started.", Prefix(logPrefix));
        _log.LogTrace("{LogPrefix}SQL GetTargetColumns: {Sql}", Prefix(logPrefix), sql);
        _log.LogTrace("{LogPrefix}SQL GetTargetColumns Params: {Params}", Prefix(logPrefix), SqlLogFormatter.FormatParameters(cmd.Parameters));
        await using var rdr = await cmd.ExecuteReaderAsync();
        sw.Stop();
        _log.LogDebug("{LogPrefix}GetTargetColumns elapsed: {Elapsed}ms", Prefix(logPrefix), sw.Elapsed.TotalMilliseconds);
        while (await rdr.ReadAsync())
            set.Add(rdr.GetString(0));
        return set;
    }

    private async Task ExecAsync(SqlConnection conn, string sql, string? logPrefix = null, params (string Name, object Value)[] prms)
    {
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        foreach (var (n, v) in prms)
            cmd.Parameters.AddWithValue(n, v ?? DBNull.Value);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("{LogPrefix}SQL Exec started.", Prefix(logPrefix));
        _log.LogTrace("{LogPrefix}SQL Exec: {Sql}", Prefix(logPrefix), sql);
        _log.LogTrace("{LogPrefix}SQL Exec Params: {Params}", Prefix(logPrefix), SqlLogFormatter.FormatParameters(cmd.Parameters));
        await cmd.ExecuteNonQueryAsync();
        sw.Stop();
        _log.LogDebug("{LogPrefix}Exec elapsed: {Elapsed}ms", Prefix(logPrefix), sw.Elapsed.TotalMilliseconds);
    }

    private async Task EnsureMetadataColumnsAsync(SqlConnection conn, string schema, string table, string? logPrefix = null)
    {
        var cols = await GetTargetColumnsAsync(conn, schema, table, logPrefix);

        if (!cols.Contains("_sg_update_datetime"))
        {
            var sql = $@"ALTER TABLE [{schema}].[{table}] ADD [_sg_update_datetime] datetime2(0) NULL;";
            await ExecAsync(conn, sql, logPrefix);
        }

        if (!cols.Contains("_sg_update_op"))
        {
            var sql = $@"ALTER TABLE [{schema}].[{table}] ADD [_sg_update_op] char(1) NULL;";
            await ExecAsync(conn, sql, logPrefix);
        }
    }

    private static string Prefix(string? logPrefix) =>
        string.IsNullOrWhiteSpace(logPrefix) ? "[app] " : $"{logPrefix} ";
}

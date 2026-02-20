using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Pluck.Source;
using Pluck.Util;

namespace Pluck.Target;

public sealed class SqlServerDestinationSchemaManager
{
    private readonly SqlServerDestinationConnectionFactory _factory;
    private readonly ILogger _log;

    public SqlServerDestinationSchemaManager(SqlServerDestinationConnectionFactory factory, ILogger? log = null)
    {
        _factory = factory;
        _log = log ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
    }

    public async Task EnsureTableAndSchemaAsync(string schema, string table, List<SourceColumn> sourceColumns, List<string> primaryKey, string? logPrefix = null)
    {
        await using var conn = await _factory.OpenAsync(logPrefix: logPrefix);

        var ensureSchemaSql = @"
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = @schema)
    EXEC('CREATE SCHEMA [' + @schema + ']');
";
        await ExecAsync(conn, ensureSchemaSql, logPrefix, ("@schema", schema));

        var colDefs = string.Join(",\n",
            sourceColumns.Select(c => $"[{c.Name}] {TypeMapper.SqlServerToSqlServerType(c.SqlServerTypeName)} NULL"));

        var createSql = $@"
IF OBJECT_ID(@fullName, 'U') IS NULL
BEGIN
    EXEC('CREATE TABLE [{schema}].[{table}] (
{colDefs}
    )');
END
";
        await ExecAsync(conn, createSql, logPrefix, ("@fullName", $"{schema}.{table}"));

        var targetCols = await GetTargetColumnsAsync(conn, schema, table, logPrefix);
        var missing = sourceColumns
            .Where(sc => !targetCols.Contains(sc.Name, StringComparer.OrdinalIgnoreCase))
            .ToList();

        foreach (var m in missing)
        {
            var addSql = $@"ALTER TABLE [{schema}].[{table}] ADD [{m.Name}] {TypeMapper.SqlServerToSqlServerType(m.SqlServerTypeName)} NULL;";
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
        _log.LogDebug("{LogPrefix}GetTargetColumns execution started.", Prefix(logPrefix));
        await using var rdr = await cmd.ExecuteReaderAsync();
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

        _log.LogTrace("{LogPrefix}SQL Exec: {Sql}", Prefix(logPrefix), sql);
        _log.LogTrace("{LogPrefix}SQL Exec Params: {Params}", Prefix(logPrefix), SqlLogFormatter.FormatParameters(cmd.Parameters));
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task EnsureMetadataColumnsAsync(SqlConnection conn, string schema, string table, string? logPrefix = null)
    {
        var cols = await GetTargetColumnsAsync(conn, schema, table, logPrefix);

        if (!cols.Contains("_pluck_update_datetime"))
        {
            var sql = $@"ALTER TABLE [{schema}].[{table}] ADD [_pluck_update_datetime] datetime2(0) NULL;";
            await ExecAsync(conn, sql, logPrefix);
        }

        if (!cols.Contains("_pluck_update_op"))
        {
            var sql = $@"ALTER TABLE [{schema}].[{table}] ADD [_pluck_update_op] char(1) NULL;";
            await ExecAsync(conn, sql, logPrefix);
        }
    }

    private static string Prefix(string? logPrefix) =>
        string.IsNullOrWhiteSpace(logPrefix) ? "[app] " : $"{logPrefix} ";
}

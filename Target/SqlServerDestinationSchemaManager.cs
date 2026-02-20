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

    public async Task EnsureTableAndSchemaAsync(
        string schema,
        string table,
        List<SourceColumn> sourceColumns,
        List<string> primaryKey,
        bool createClusteredColumnstoreOnCreate = false,
        string? logPrefix = null)
    {
        await using var conn = await _factory.OpenAsync(logPrefix: logPrefix);
        var tableExistedBefore = await TableExistsAsync(conn, schema, table, logPrefix);

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

        if (!tableExistedBefore && createClusteredColumnstoreOnCreate)
            await EnsureClusteredColumnstoreIndexAsync(conn, schema, table, logPrefix);
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
        LogSql("get target columns", cmd, logPrefix);
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

        LogSql("exec", cmd, logPrefix);
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

    private async Task<bool> TableExistsAsync(SqlConnection conn, string schema, string table, string? logPrefix = null)
    {
        var sql = @"
SELECT 1
FROM sys.tables t
INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = @schema AND t.name = @table;
";
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _factory.CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", table);
        LogSql("table exists check", cmd, logPrefix);
        var result = await cmd.ExecuteScalarAsync();
        return result is not null;
    }

    private async Task EnsureClusteredColumnstoreIndexAsync(SqlConnection conn, string schema, string table, string? logPrefix = null)
    {
        var sql = $@"
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes i
    INNER JOIN sys.tables t ON t.object_id = i.object_id
    INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = @schema
      AND t.name = @table
      AND i.name = '__pluck_cci'
)
BEGIN
    CREATE CLUSTERED COLUMNSTORE INDEX [__pluck_cci] ON [{schema}].[{table}];
END;
";
        await ExecAsync(conn, sql, logPrefix, ("@schema", schema), ("@table", table));
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

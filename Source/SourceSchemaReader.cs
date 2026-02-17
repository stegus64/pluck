using System.Data;
using Microsoft.Data.SqlClient;
using FabricIncrementalReplicator.Config;
using FabricIncrementalReplicator.Util;

namespace FabricIncrementalReplicator.Source;

using Microsoft.Extensions.Logging;

public sealed class SourceSchemaReader
{
    private readonly string _connString;
    private readonly SchemaDiscoveryConfig _cfg;
    private readonly ILogger<SourceSchemaReader> _log;
    public SourceSchemaReader(string connString, SchemaDiscoveryConfig cfg, ILogger<SourceSchemaReader>? log = null)
    {
        _connString = connString;
        _cfg = cfg;
        _log = log ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<SourceSchemaReader>.Instance;
    }

    public async Task<List<SourceColumn>> DescribeQueryAsync(string sourceSql)
    {
        if (string.Equals(_cfg.Mode, "FmtOnly", StringComparison.OrdinalIgnoreCase))
        {
            var cols = await TryDescribeByFmtOnlyAsync(sourceSql);
            if (cols.Count > 0) return cols;
        }

        // Preferred
        var described = await TryDescribeBySpDescribeFirstResultSetAsync(sourceSql);
        if (described.Count > 0) return described;

        // Fallback
        var fallback = await TryDescribeByFmtOnlyAsync(sourceSql);
        if (fallback.Count > 0) return fallback;

        throw new Exception("Could not derive schema from sourceSql using either DescribeFirstResultSet or FMTONLY.");
    }

    private async Task<List<SourceColumn>> TryDescribeBySpDescribeFirstResultSetAsync(string sourceSql)
    {
        // sp_describe_first_result_set returns metadata without executing the query.
        const string sql = @"
EXEC sp_describe_first_result_set @tsql = @q, @params = NULL, @browse_information_mode = 1;
";
        await using var conn = new SqlConnection(_connString);
        await conn.OpenAsync();

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@q", sourceSql);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("SQL Describe (sp_describe_first_result_set): {Sql}", sql);
        _log.LogDebug("SQL Describe Params: {Params}", SqlLogFormatter.FormatParameters(cmd.Parameters));
        var result = new List<SourceColumn>();
        await using var rdr = await cmd.ExecuteReaderAsync();
        sw.Stop();
        _log.LogDebug("Describe execution time: {Elapsed}ms", sw.Elapsed.TotalMilliseconds);

        var nameOrd = rdr.GetOrdinal("name");
        var isHiddenOrd = rdr.GetOrdinal("is_hidden");
        var sysTypeOrd = rdr.GetOrdinal("system_type_name");

        while (await rdr.ReadAsync())
        {
            if (!rdr.IsDBNull(isHiddenOrd) && rdr.GetBoolean(isHiddenOrd))
                continue;

            var name = rdr.GetString(nameOrd);
            var systemType = rdr.GetString(sysTypeOrd);

            result.Add(SourceColumn.FromSystemTypeName(name, systemType));
        }

        return result;
    }

    private async Task<List<SourceColumn>> TryDescribeByFmtOnlyAsync(string sourceSql)
    {
        // FMTONLY is deprecated, but used here because the user requested it as an approach.
        // We execute: SET FMTONLY ON; <query>; SET FMTONLY OFF;
        var sql = $"SET FMTONLY ON; {sourceSql}; SET FMTONLY OFF;";

        await using var conn = new SqlConnection(_connString);
        await conn.OpenAsync();

        await using var cmd = new SqlCommand(sql, conn);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("SQL FMTONLY describe: {SqlSnippet}", sourceSql.Length > 200 ? sourceSql[..200] + "..." : sourceSql);
        await using var rdr = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
        sw.Stop();
        _log.LogDebug("FMTONLY execution time: {Elapsed}ms", sw.Elapsed.TotalMilliseconds);

        var schema = rdr.GetSchemaTable();
        if (schema == null) return new();

        var cols = new List<SourceColumn>();
        foreach (System.Data.DataRow row in schema.Rows)
        {
            var colName = row.Field<string>("ColumnName") ?? "";
            var dataType = row.Field<Type>("DataType");
            var providerType = row["ProviderType"];

            cols.Add(SourceColumn.FromAdoSchema(colName, dataType, providerType));
        }
        return cols;
    }
}

public sealed record SourceColumn(
    string Name,
    string SqlServerTypeName,  // e.g. "datetime2(7)", "int", "nvarchar(100)"
    bool IsNullable
)
{
    public static SourceColumn FromSystemTypeName(string name, string systemTypeName)
    {
        // system_type_name often includes nullability and collation; we keep it simple.
        // Example: "nvarchar(100)"
        return new SourceColumn(name, Normalize(systemTypeName), IsNullable: true);
    }

    public static SourceColumn FromAdoSchema(string name, Type? dataType, object? providerType)
    {
        // This is best-effort. In practice, DescribeFirstResultSet is more reliable.
        var type = dataType?.Name ?? "Object";
        // Minimal mapping; TypeMapper handles real DDL mapping later.
        var sqlType = TypeMapper.AdoTypeToSqlServerType(type);
        return new SourceColumn(name, sqlType, IsNullable: true);
    }

    private static string Normalize(string systemTypeName)
    {
        // strip "NULL"/"NOT NULL" if present
        return systemTypeName
            .Replace("NOT NULL", "", StringComparison.OrdinalIgnoreCase)
            .Replace("NULL", "", StringComparison.OrdinalIgnoreCase)
            .Trim();
    }
}

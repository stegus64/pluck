using FabricIncrementalReplicator.Source;
using FabricIncrementalReplicator.Util;
using Microsoft.Data.SqlClient;

namespace FabricIncrementalReplicator.Target;

public sealed class WarehouseSchemaManager
{
    private readonly WarehouseConnectionFactory _factory;
    public WarehouseSchemaManager(WarehouseConnectionFactory factory) => _factory = factory;

    public async Task EnsureTableAndSchemaAsync(string schema, string table, List<SourceColumn> sourceColumns, List<string> primaryKey)
    {
        await using var conn = await _factory.OpenAsync();

        // Ensure schema exists
        var ensureSchemaSql = $@"
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = @schema)
    EXEC('CREATE SCHEMA [{schema}]');
";
        await ExecAsync(conn, ensureSchemaSql, ("@schema", schema));

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
        await ExecAsync(conn, createSql, ("@fullName", $"{schema}.{table}"));

        // Add missing columns
        var targetCols = await GetTargetColumnsAsync(conn, schema, table);

        var missing = sourceColumns
            .Where(sc => !targetCols.Contains(sc.Name, StringComparer.OrdinalIgnoreCase))
            .ToList();

        foreach (var m in missing)
        {
            var addSql = $@"ALTER TABLE [{schema}].[{table}] ADD [{m.Name}] {TypeMapper.SqlServerToFabricWarehouseType(m.SqlServerTypeName)} NULL;";
            await ExecAsync(conn, addSql);
        }
    }

    private static async Task<HashSet<string>> GetTargetColumnsAsync(SqlConnection conn, string schema, string table)
    {
        var sql = @"
SELECT c.name
FROM sys.columns c
JOIN sys.tables t ON t.object_id = c.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = @schema AND t.name = @table;
";
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", table);

        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
            set.Add(rdr.GetString(0));
        return set;
    }

    private static async Task ExecAsync(SqlConnection conn, string sql, params (string Name, object Value)[] prms)
    {
        await using var cmd = new SqlCommand(sql, conn);
        foreach (var (n, v) in prms)
            cmd.Parameters.AddWithValue(n, v ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
    }
}
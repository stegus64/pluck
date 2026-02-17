using FabricIncrementalReplicator.Config;
using FabricIncrementalReplicator.Source;
using FabricIncrementalReplicator.Util;
using Microsoft.Data.SqlClient;

namespace FabricIncrementalReplicator.Target;

public sealed class WarehouseLoader
{
    private readonly WarehouseConnectionFactory _factory;
    public WarehouseLoader(WarehouseConnectionFactory factory) => _factory = factory;

    public async Task<object?> GetMaxUpdateKeyAsync(string schema, string table, string updateKey)
    {
        await using var conn = await _factory.OpenAsync();
        var sql = $"SELECT MAX([{updateKey}]) FROM [{schema}].[{table}];";
        await using var cmd = new SqlCommand(sql, conn);
        var v = await cmd.ExecuteScalarAsync();
        return v is DBNull ? null : v;
    }

    public async Task LoadAndMergeAsync(
        string targetSchema,
        string targetTable,
        string tempTable,
        List<SourceColumn> columns,
        List<string> primaryKey,
        string oneLakeDfsUrl,
        CleanupConfig cleanup)
    {
        await using var conn = await _factory.OpenAsync();
        await using var tx = conn.BeginTransaction();

        // 1) Create temp table
        var colDefs = string.Join(",\n", columns.Select(c =>
            $"[{c.Name}] {TypeMapper.SqlServerToFabricWarehouseType(c.SqlServerTypeName)} NULL"));

        var createTempSql = $@"
CREATE TABLE [{targetSchema}].[{tempTable}] (
{colDefs}
);
";
        await ExecAsync(conn, tx, createTempSql);

        // 2) COPY INTO from OneLake (CSV + gzip supported; OneLake supported as source in Fabric) :contentReference[oaicite:8]{index=8}
        var colList = string.Join(",", columns.Select(c => $"[{c.Name}]"));
        var copySql = $@"
COPY INTO [{targetSchema}].[{tempTable}] ({colList})
FROM '{oneLakeDfsUrl}'
WITH (
    FILE_TYPE = 'CSV',
    COMPRESSION = 'GZIP',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A'
);
";
        await ExecAsync(conn, tx, copySql);

        // 3) MERGE into target (MERGE supported in Fabric Warehouse per official surface area) :contentReference[oaicite:9]{index=9}
        var mergeSql = MergeBuilder.BuildMergeSql(targetSchema, targetTable, tempTable, columns, primaryKey);
        await ExecAsync(conn, tx, mergeSql);

        // 4) Cleanup temp table
        if (cleanup.DropTempTables)
        {
            var dropSql = $@"DROP TABLE [{targetSchema}].[{tempTable}];";
            await ExecAsync(conn, tx, dropSql);
        }

        await tx.CommitAsync();
    }

    private static async Task ExecAsync(SqlConnection conn, SqlTransaction tx, string sql)
    {
        await using var cmd = new SqlCommand(sql, conn, tx);
        await cmd.ExecuteNonQueryAsync();
    }
}
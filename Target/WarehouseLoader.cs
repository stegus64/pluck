using FabricIncrementalReplicator.Source;
using FabricIncrementalReplicator.Config;
using FabricIncrementalReplicator.Util;
using Microsoft.Data.SqlClient;

namespace FabricIncrementalReplicator.Target;

public sealed class WarehouseLoader
{
    private readonly WarehouseConnectionFactory _factory;

    public WarehouseLoader(WarehouseConnectionFactory factory) => _factory = factory;

    /// <summary>
    /// Get the maximum value of the update key from the target table to use as watermark.
    /// </summary>
    public async Task<object?> GetMaxUpdateKeyAsync(string schema, string table, string updateKey, CancellationToken ct = default)
    {
        await using var conn = await _factory.OpenAsync(ct);

        var sql = $"SELECT MAX([{updateKey}]) FROM [{schema}].[{table}];";

        await using var cmd = new SqlCommand(sql, conn);
        var result = await cmd.ExecuteScalarAsync(ct);

        return result == DBNull.Value ? null : result;
    }

    /// <summary>
    /// Load data from OneLake staging via COPY INTO, then perform MERGE to synchronize with target.
    /// </summary>
    public async Task LoadAndMergeAsync(
        string targetSchema,
        string targetTable,
        string tempTable,
        List<SourceColumn> columns,
        List<string> primaryKey,
        string oneLakeDfsUrl,
        CleanupConfig cleanup,
        CancellationToken ct = default)
    {
        await using var conn = await _factory.OpenAsync(ct);

        try
        {
            // 1. Load into temp table via COPY INTO
            await CopyIntoTempTableAsync(conn, targetSchema, tempTable, columns, oneLakeDfsUrl, ct);

            // 2. Perform MERGE
            var mergeBuilder = new MergeBuilder();
            var mergeSql = mergeBuilder.BuildMergeSql(
                targetSchema,
                targetTable,
                tempTable,
                columns,
                primaryKey
            );

            await ExecAsync(conn, mergeSql, ct);
        }
        finally
        {
            // Cleanup temp table if requested
            if (cleanup.DropTempTables)
            {
                var dropSql = $"DROP TABLE IF EXISTS [{targetSchema}].[{tempTable}];";
                try
                {
                    await ExecAsync(conn, dropSql, ct);
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }
        }
    }

    private async Task CopyIntoTempTableAsync(
        SqlConnection conn,
        string schema,
        string tempTable,
        List<SourceColumn> columns,
        string oneLakeDfsUrl,
        CancellationToken ct)
    {
        // Create temp table
        var colDefs = string.Join(",\n",
            columns.Select(c => $"[{c.Name}] {TypeMapper.SqlServerToFabricWarehouseType(c.SqlServerTypeName)} NULL"));

        var createTableSql = $@"
CREATE TABLE [{schema}].[{tempTable}] (
{colDefs}
);
";

        await ExecAsync(conn, createTableSql, ct);

        // COPY INTO from OneLake (CSV.GZ format)
        var copyIntoSql = $@"
COPY INTO [{schema}].[{tempTable}]
    ({string.Join(", ", columns.Select(c => $"[{c.Name}]"))})
FROM '{oneLakeDfsUrl}'
WITH (
    FILE_TYPE = 'CSV',
    CREDENTIAL = (IDENTITY = 'Shared Access Signature', SECRET = '?sp=racwd&st=...')
);
";

        // Note: The CREDENTIAL part above is a placeholder. 
        // In practice, Fabric Warehouse uses Entra ID, so you may not need explicit credentials
        // For now, we'll use a simpler COPY INTO without the credential for Fabric Warehouse
        var simpleCopyIntoSql = $@"
COPY INTO [{schema}].[{tempTable}]
    ({string.Join(", ", columns.Select(c => $"[{c.Name}]"))})
FROM '{oneLakeDfsUrl}'
WITH (
    FILE_TYPE = 'CSV',
    COMPRESSION = 'GZIP'
);
";

        await ExecAsync(conn, simpleCopyIntoSql, ct);
    }

    private static async Task ExecAsync(SqlConnection conn, string sql, CancellationToken ct = default)
    {
        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = 0; // No timeout for long-running operations
        await cmd.ExecuteNonQueryAsync(ct);
    }
}

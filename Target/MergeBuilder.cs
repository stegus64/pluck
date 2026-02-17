using FabricIncrementalReplicator.Source;
using FabricIncrementalReplicator.Util;

namespace FabricIncrementalReplicator.Target;

public sealed class MergeBuilder
{
    public string BuildMergeSql(
        string targetSchema,
        string targetTable,
        string tempTable,
        List<SourceColumn> columns,
        List<string> primaryKey)
    {
        // Build ON condition based on primary key
        var onCondition = string.Join(" AND ",
            primaryKey.Select(pk => $"t.[{pk}] = s.[{pk}]"));

        // Build SET clause for UPDATE (all non-key columns)
        var setClause = string.Join(",\n    ",
            columns
                .Where(c => !primaryKey.Contains(c.Name, StringComparer.OrdinalIgnoreCase))
                .Select(c => $"t.[{c.Name}] = s.[{c.Name}]"));

        // Build column list for INSERT
        var insertColumns = string.Join(", ", columns.Select(c => $"[{c.Name}]"));
        var selectColumns = string.Join(", ", columns.Select(c => $"s.[{c.Name}]"));

        var mergeSql = $@"
MERGE [{targetSchema}].[{targetTable}] t
USING [{targetSchema}].[{tempTable}] s
    ON {onCondition}
WHEN MATCHED THEN
    UPDATE SET
    {setClause}
WHEN NOT MATCHED THEN
    INSERT ({insertColumns})
    VALUES ({selectColumns});
";

        return mergeSql;
    }
}

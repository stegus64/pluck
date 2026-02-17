using FabricIncrementalReplicator.Source;

namespace FabricIncrementalReplicator.Target;

public static class MergeBuilder
{
    public static string BuildMergeSql(
        string schema,
        string targetTable,
        string tempTable,
        List<SourceColumn> columns,
        List<string> primaryKey)
    {
        var on = string.Join(" AND ", primaryKey.Select(pk => $"t.[{pk}] = s.[{pk}]"));

        var nonPkCols = columns
            .Select(c => c.Name)
            .Where(c => !primaryKey.Contains(c, StringComparer.OrdinalIgnoreCase))
            .ToList();

        var setClause = nonPkCols.Count == 0
            ? "/* no non-PK columns */"
            : string.Join(", ", nonPkCols.Select(c => $"t.[{c}] = s.[{c}]"));

        var insertCols = string.Join(", ", columns.Select(c => $"[{c.Name}]"));
        var insertVals = string.Join(", ", columns.Select(c => $"s.[{c.Name}]"));

        return $@"
MERGE [{schema}].[{targetTable}] AS t
USING [{schema}].[{tempTable}] AS s
ON {on}
WHEN MATCHED THEN
    UPDATE SET {setClause}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insertCols})
    VALUES ({insertVals});
";
    }
}
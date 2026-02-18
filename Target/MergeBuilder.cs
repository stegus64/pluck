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

        var setAssignments = new List<string>();
        setAssignments.AddRange(nonPkCols.Select(c => $"t.[{c}] = s.[{c}]"));
        setAssignments.Add("t.[_sg_update_datetime] = SYSUTCDATETIME()");
        setAssignments.Add("t.[_sg_update_op] = 'U'");
        var setClause = string.Join(", ", setAssignments);

        var insertCols = string.Join(", ", columns.Select(c => $"[{c.Name}]").Concat(["[_sg_update_datetime]", "[_sg_update_op]"]));
        var insertVals = string.Join(", ", columns.Select(c => $"s.[{c.Name}]").Concat(["SYSUTCDATETIME()", "'I'"]));

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

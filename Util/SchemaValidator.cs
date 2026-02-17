using FabricIncrementalReplicator.Source;

namespace FabricIncrementalReplicator.Util;

public static class SchemaValidator
{
    public static void EnsureContainsColumns(List<SourceColumn> cols, List<string> pk, string updateKey)
    {
        var set = new HashSet<string>(cols.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);

        foreach (var c in pk)
            if (!set.Contains(c))
                throw new Exception($"Source query does not return PK column '{c}'. Ensure your SELECT includes it.");

        if (!set.Contains(updateKey))
            throw new Exception($"Source query does not return updateKey column '{updateKey}'. Ensure your SELECT includes it.");
    }
}
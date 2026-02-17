namespace FabricIncrementalReplicator.Util;

public static class TypeMapper
{
    // Basic mapping: SQL Server -> Fabric Warehouse types (mostly compatible T-SQL).
    public static string SqlServerToFabricWarehouseType(string sqlServerType)
    {
        var t = sqlServerType.Trim().ToLowerInvariant();

        // pass-through common types
        if (t.StartsWith("nvarchar") || t.StartsWith("varchar") || t.StartsWith("nchar") || t.StartsWith("char"))
            return t;
        if (t.StartsWith("varbinary") || t.StartsWith("binary"))
            return t;
        if (t.StartsWith("decimal") || t.StartsWith("numeric"))
            return t;
        if (t.StartsWith("datetime2") || t.StartsWith("datetime") || t.StartsWith("date") || t.StartsWith("time"))
            return t;
        if (t is "int" or "bigint" or "smallint" or "tinyint" or "bit" or "float" or "real" or "uniqueidentifier")
            return t;

        // Fallback to nvarchar(max) for unknown expression types
        return "nvarchar(max)";
    }

    public static string AdoTypeToSqlServerType(string adoTypeName)
    {
        return adoTypeName.ToLowerInvariant() switch
        {
            "string" => "nvarchar(max)",
            "int32" => "int",
            "int64" => "bigint",
            "datetime" => "datetime2(7)",
            "decimal" => "decimal(38, 10)",
            "boolean" => "bit",
            "double" => "float",
            "guid" => "uniqueidentifier",
            _ => "nvarchar(max)"
        };
    }
}
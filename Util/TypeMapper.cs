namespace FabricIncrementalReplicator.Util;

public static class TypeMapper
{
    private const int FabricVarcharMaxBytes = 8000;

    // Basic mapping: SQL Server -> Fabric Warehouse types (mostly compatible T-SQL).
    public static string SqlServerToFabricWarehouseType(string sqlServerType)
    {
        var t = sqlServerType.Trim().ToLowerInvariant();

        // This warehouse endpoint rejects Unicode string types (nchar/nvarchar).
        // Normalize them to varchar-compatible equivalents and scale length in bytes.
        if (t.StartsWith("nvarchar("))
            return ConvertUnicodeTypeToVarchar(t, "nvarchar");
        if (t.StartsWith("nchar("))
            return ConvertUnicodeTypeToVarchar(t, "nchar");
        if (t.StartsWith("char("))
            return "varchar" + t["char".Length..];

        // pass-through common types
        if (t.StartsWith("varchar"))
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

    private static string ConvertUnicodeTypeToVarchar(string normalizedType, string sourceTypeName)
    {
        var openParen = normalizedType.IndexOf('(');
        var closeParen = normalizedType.IndexOf(')', openParen + 1);
        if (openParen < 0 || closeParen <= openParen)
            return "varchar(max)";

        var sizeToken = normalizedType.Substring(openParen + 1, closeParen - openParen - 1).Trim();
        if (sizeToken.Equals("max", StringComparison.OrdinalIgnoreCase))
            return "varchar(max)";

        if (!int.TryParse(sizeToken, out var unicodeLength) || unicodeLength <= 0)
            throw new ArgumentException($"Invalid {sourceTypeName} length in type '{normalizedType}'.");

        var varcharLength = Math.Min(unicodeLength * 2, FabricVarcharMaxBytes);
        return $"varchar({varcharLength})";
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

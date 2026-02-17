using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;

namespace FabricIncrementalReplicator.Util;

public static class SqlLogFormatter
{
    public static string FormatParameters(SqlParameterCollection parameters, int maxValueChars = 256)
    {
        if (parameters.Count == 0)
            return "(none)";

        var parts = new List<string>(parameters.Count);
        foreach (SqlParameter p in parameters)
            parts.Add($"{p.ParameterName}={FormatValue(p.Value, maxValueChars)}");

        return string.Join(", ", parts);
    }

    private static string FormatValue(object? value, int maxValueChars)
    {
        if (value is null || value == DBNull.Value)
            return "NULL";

        if (value is byte[] bytes)
            return $"<byte[{bytes.Length}]>";

        var text = value switch
        {
            DateTime dt => dt.ToString("o", CultureInfo.InvariantCulture),
            DateTimeOffset dto => dto.ToString("o", CultureInfo.InvariantCulture),
            string s => s,
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? value.ToString() ?? string.Empty
        };

        if (text.Length > maxValueChars)
            text = $"{text[..maxValueChars]}...({text.Length} chars)";

        var sb = new StringBuilder(text.Length + 2);
        sb.Append('\'');
        sb.Append(text.Replace("'", "''", StringComparison.Ordinal));
        sb.Append('\'');
        return sb.ToString();
    }
}

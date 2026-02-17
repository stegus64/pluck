using Microsoft.Data.SqlClient;

namespace FabricIncrementalReplicator.Source;

public sealed class SourceChunkReader
{
    private readonly string _connString;
    public SourceChunkReader(string connString) => _connString = connString;

    public async Task<(object? Min, object? Max)> GetMinMaxUpdateKeyAsync(string sourceSql, string updateKey, object? watermark)
    {
        var sql = $@"
WITH src AS (
    {sourceSql}
)
SELECT MIN([{updateKey}]) AS MinVal, MAX([{updateKey}]) AS MaxVal
FROM src
WHERE [{updateKey}] > @watermark;
";
        await using var conn = new SqlConnection(_connString);
        await conn.OpenAsync();

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@watermark", watermark ?? DBNull.Value);

        await using var rdr = await cmd.ExecuteReaderAsync();
        if (!await rdr.ReadAsync()) return (null, null);

        return (rdr.IsDBNull(0) ? null : rdr.GetValue(0),
                rdr.IsDBNull(1) ? null : rdr.GetValue(1));
    }

    public async Task<List<Dictionary<string, object?>>> ReadNextChunkAsync(
        string sourceSql,
        List<SourceColumn> columns,
        string updateKey,
        List<string> primaryKey,
        object? watermark,
        int chunkSize)
    {
        var orderBy = string.Join(", ",
            new[] { updateKey }.Concat(primaryKey).Select(c => $"[{c}] ASC"));

        var sql = $@"
WITH src AS (
    {sourceSql}
)
SELECT TOP (@chunkSize) *
FROM src
WHERE [{updateKey}] > @watermark
ORDER BY {orderBy};
";

        await using var conn = new SqlConnection(_connString);
        await conn.OpenAsync();

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@chunkSize", chunkSize);
        cmd.Parameters.AddWithValue("@watermark", watermark ?? DBNull.Value);

        var rows = new List<Dictionary<string, object?>>();
        await using var rdr = await cmd.ExecuteReaderAsync();

        while (await rdr.ReadAsync())
        {
            var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < rdr.FieldCount; i++)
            {
                var name = rdr.GetName(i);
                row[name] = rdr.IsDBNull(i) ? null : rdr.GetValue(i);
            }
            rows.Add(row);
        }

        return rows;
    }

    public static object? GetMaxValue(List<Dictionary<string, object?>> rows, string column)
    {
        object? max = null;
        foreach (var r in rows)
        {
            if (!r.TryGetValue(column, out var v) || v is null) continue;
            if (max is null || Comparer<object>.Default.Compare(v, max) > 0)
                max = v;
        }
        return max;
    }
}
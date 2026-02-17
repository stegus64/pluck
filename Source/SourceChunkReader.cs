using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace FabricIncrementalReplicator.Source;

public sealed class SourceChunkReader
{
    private readonly string _connString;
    private readonly Microsoft.Extensions.Logging.ILogger<SourceChunkReader> _log;
    public SourceChunkReader(string connString, Microsoft.Extensions.Logging.ILogger<SourceChunkReader>? log = null)
    {
        _connString = connString;
        _log = log ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<SourceChunkReader>.Instance;
    }

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
        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("SQL GetMinMax: {Sql}", sql);
        await using var rdr = await cmd.ExecuteReaderAsync();
        sw.Stop();
        _log.LogDebug("GetMinMax execution time: {Elapsed}ms", sw.Elapsed.TotalMilliseconds);
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
        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("SQL ReadNextChunk: {Sql}", sql);
        await using var rdr = await cmd.ExecuteReaderAsync();
        sw.Stop();
        _log.LogDebug("ReadNextChunk execution time: {Elapsed}ms", sw.Elapsed.TotalMilliseconds);

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
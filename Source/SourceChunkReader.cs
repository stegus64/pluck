using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;
using FabricIncrementalReplicator.Util;

namespace FabricIncrementalReplicator.Source;

public sealed class SourceChunkReader
{
    private readonly string _connString;
    private readonly int _commandTimeoutSeconds;
    private readonly Microsoft.Extensions.Logging.ILogger<SourceChunkReader> _log;
    public SourceChunkReader(string connString, int commandTimeoutSeconds = 3600, Microsoft.Extensions.Logging.ILogger<SourceChunkReader>? log = null)
    {
        _connString = connString;
        _commandTimeoutSeconds = commandTimeoutSeconds > 0 ? commandTimeoutSeconds : 3600;
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
WHERE (@watermark IS NULL OR [{updateKey}] > @watermark);
";
        await using var conn = new SqlConnection(_connString);
        await conn.OpenAsync();

        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _commandTimeoutSeconds;
        cmd.Parameters.AddWithValue("@watermark", watermark ?? DBNull.Value);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("SQL GetMinMax: {Sql}", sql);
        _log.LogDebug("SQL GetMinMax Params: {Params}", SqlLogFormatter.FormatParameters(cmd.Parameters));
        await using var rdr = await cmd.ExecuteReaderAsync();
        sw.Stop();
        _log.LogDebug("GetMinMax execution time: {Elapsed}ms", sw.Elapsed.TotalMilliseconds);
        if (!await rdr.ReadAsync()) return (null, null);

        return (rdr.IsDBNull(0) ? null : rdr.GetValue(0),
                rdr.IsDBNull(1) ? null : rdr.GetValue(1));
    }

    public async IAsyncEnumerable<object?[]> ReadChunkByIntervalAsync(
        string sourceSql,
        List<SourceColumn> columns,
        string updateKey,
        object lowerBoundInclusive,
        object upperBoundExclusive,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var selectList = string.Join(", ", columns.Select(c => $"[{c.Name}]"));

        var sql = $@"
WITH src AS (
    {sourceSql}
)
SELECT {selectList}
FROM src
WHERE [{updateKey}] >= @lowerBound
  AND [{updateKey}] < @upperBound;
";

        await using var conn = new SqlConnection(_connString);
        await conn.OpenAsync(ct);

        await using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = _commandTimeoutSeconds;
        cmd.Parameters.AddWithValue("@lowerBound", lowerBoundInclusive);
        cmd.Parameters.AddWithValue("@upperBound", upperBoundExclusive);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("SQL ReadChunkByInterval: {Sql}", sql);
        _log.LogDebug("SQL ReadChunkByInterval Params: {Params}", SqlLogFormatter.FormatParameters(cmd.Parameters));
        await using var rdr = await cmd.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess, ct);
        sw.Stop();
        _log.LogDebug("ReadChunkByInterval execution time: {Elapsed}ms", sw.Elapsed.TotalMilliseconds);

        while (await rdr.ReadAsync(ct))
        {
            var values = new object?[columns.Count];
            for (int i = 0; i < columns.Count; i++)
                values[i] = rdr.IsDBNull(i) ? null : rdr.GetValue(i);

            yield return values;
        }
    }
}

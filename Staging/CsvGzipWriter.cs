using System.Globalization;
using System.IO.Compression;
using CsvHelper;
using CsvHelper.Configuration;
using FabricIncrementalReplicator.Source;

namespace FabricIncrementalReplicator.Staging;

public sealed record ChunkWriteResult(int RowCount, object? MaxUpdateKey);

public sealed class CsvGzipWriter
{
    public Task<ChunkWriteResult> WriteCsvAsync(
        string path,
        List<SourceColumn> columns,
        IAsyncEnumerable<object?[]> rows,
        int updateKeyIndex,
        CancellationToken ct = default)
        => WriteCsvInternalAsync(path, columns, rows, updateKeyIndex, compress: false, ct);

    public async Task<ChunkWriteResult> WriteCsvGzAsync(
        string path,
        List<SourceColumn> columns,
        IAsyncEnumerable<object?[]> rows,
        int updateKeyIndex,
        CancellationToken ct = default)
        => await WriteCsvInternalAsync(path, columns, rows, updateKeyIndex, compress: true, ct);

    private static async Task<ChunkWriteResult> WriteCsvInternalAsync(
        string path,
        List<SourceColumn> columns,
        IAsyncEnumerable<object?[]> rows,
        int updateKeyIndex,
        bool compress,
        CancellationToken ct)
    {
        await using var fs = File.Create(path);
        await using var output = compress
            ? new GZipStream(fs, CompressionLevel.Optimal, leaveOpen: false) as Stream
            : fs;
        await using var sw = new StreamWriter(output);

        var cfg = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            NewLine = "\n"
        };

        await using var csv = new CsvWriter(sw, cfg);

        // Header
        foreach (var c in columns)
            csv.WriteField(c.Name);
        await csv.NextRecordAsync();

        // Rows
        var rowCount = 0;
        object? maxUpdateKey = null;

        await foreach (var r in rows.WithCancellation(ct))
        {
            for (int i = 0; i < columns.Count; i++)
                csv.WriteField(r[i]);

            await csv.NextRecordAsync();
            rowCount++;

            if (updateKeyIndex >= 0 && updateKeyIndex < r.Length && r[updateKeyIndex] is not null)
                maxUpdateKey = r[updateKeyIndex];
        }
        await sw.FlushAsync();
        return new ChunkWriteResult(rowCount, maxUpdateKey);
    }
}

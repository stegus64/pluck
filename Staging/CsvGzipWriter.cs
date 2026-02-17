using System.Globalization;
using System.IO.Compression;
using CsvHelper;
using CsvHelper.Configuration;
using FabricIncrementalReplicator.Source;

namespace FabricIncrementalReplicator.Staging;

public sealed record ChunkWriteResult(int RowCount);

public sealed class CsvGzipWriter
{
    public Task<ChunkWriteResult> WriteCsvAsync(
        string path,
        List<SourceColumn> columns,
        IAsyncEnumerable<object?[]> rows,
        CancellationToken ct = default)
        => WriteCsvInternalAsync(path, columns, rows, compress: false, ct);

    public async Task<ChunkWriteResult> WriteCsvGzAsync(
        string path,
        List<SourceColumn> columns,
        IAsyncEnumerable<object?[]> rows,
        CancellationToken ct = default)
        => await WriteCsvInternalAsync(path, columns, rows, compress: true, ct);

    private static async Task<ChunkWriteResult> WriteCsvInternalAsync(
        string path,
        List<SourceColumn> columns,
        IAsyncEnumerable<object?[]> rows,
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

        await foreach (var r in rows.WithCancellation(ct))
        {
            for (int i = 0; i < columns.Count; i++)
                csv.WriteField(r[i]);

            await csv.NextRecordAsync();
            rowCount++;
        }
        await sw.FlushAsync();
        return new ChunkWriteResult(rowCount);
    }
}

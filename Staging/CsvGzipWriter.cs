using System.Globalization;
using System.IO.Compression;
using CsvHelper;
using CsvHelper.Configuration;
using FabricIncrementalReplicator.Source;

namespace FabricIncrementalReplicator.Staging;

public sealed class CsvGzipWriter
{
    public async Task WriteCsvGzAsync(string path, List<SourceColumn> columns, List<Dictionary<string, object?>> rows)
    {
        await using var fs = File.Create(path);
        await using var gz = new GZipStream(fs, CompressionLevel.Optimal, leaveOpen: false);
        await using var sw = new StreamWriter(gz);

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
        foreach (var r in rows)
        {
            foreach (var c in columns)
            {
                r.TryGetValue(c.Name, out var val);
                csv.WriteField(val);
            }
            await csv.NextRecordAsync();
        }
        await sw.FlushAsync();
    }
}
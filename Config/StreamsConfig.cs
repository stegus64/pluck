namespace FabricIncrementalReplicator.Config;

public sealed class StreamsConfig
{
    public StreamConfig Defaults { get; set; } = new();
    public Dictionary<string, StreamConfig> Streams { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    public List<ResolvedStreamConfig> GetResolvedStreams()
    {
        var result = new List<ResolvedStreamConfig>();

        foreach (var (streamName, streamOverride) in Streams)
        {
            var merged = StreamConfig.Merge(Defaults, streamOverride, streamName);
            result.Add(merged.ToResolved(streamName));
        }

        return result;
    }
}

public sealed class StreamConfig
{
    public string? SourceSql { get; set; }
    public string? TargetTable { get; set; }
    public string? TargetSchema { get; set; }
    public List<string>? PrimaryKey { get; set; }
    public List<string>? ExcludeColumns { get; set; }
    public string? UpdateKey { get; set; }
    // Update-key interval size (not row count), e.g. "50000", "7d", "2m".
    public string? ChunkSize { get; set; }
    // Supported values: "csv.gz" (default), "csv", "parquet"
    public string? StagingFileFormat { get; set; }

    public static StreamConfig Merge(StreamConfig defaults, StreamConfig? streamOverride, string streamName)
    {
        var merged = new StreamConfig
        {
            SourceSql = streamOverride?.SourceSql ?? defaults.SourceSql,
            TargetTable = streamOverride?.TargetTable ?? defaults.TargetTable,
            TargetSchema = streamOverride?.TargetSchema ?? defaults.TargetSchema,
            PrimaryKey = streamOverride?.PrimaryKey ?? defaults.PrimaryKey,
            ExcludeColumns = streamOverride?.ExcludeColumns ?? defaults.ExcludeColumns,
            UpdateKey = streamOverride?.UpdateKey ?? defaults.UpdateKey,
            ChunkSize = streamOverride?.ChunkSize ?? defaults.ChunkSize,
            StagingFileFormat = streamOverride?.StagingFileFormat ?? defaults.StagingFileFormat
        };

        merged.SourceSql = ApplyToken(merged.SourceSql, streamName);
        merged.TargetTable = ApplyToken(merged.TargetTable, streamName);
        merged.TargetSchema = ApplyToken(merged.TargetSchema, streamName);
        merged.UpdateKey = ApplyToken(merged.UpdateKey, streamName);
        merged.StagingFileFormat = ApplyToken(merged.StagingFileFormat, streamName);

        if (merged.PrimaryKey is not null)
            merged.PrimaryKey = merged.PrimaryKey.Select(k => ApplyToken(k, streamName) ?? k).ToList();
        if (merged.ExcludeColumns is not null)
            merged.ExcludeColumns = merged.ExcludeColumns.Select(k => ApplyToken(k, streamName) ?? k).ToList();

        return merged;
    }

    public ResolvedStreamConfig ToResolved(string streamName)
    {
        if (string.IsNullOrWhiteSpace(SourceSql))
            throw new Exception($"Missing required stream setting 'sourceSql' for stream '{streamName}'.");
        if (string.IsNullOrWhiteSpace(TargetTable))
            throw new Exception($"Missing required stream setting 'targetTable' for stream '{streamName}'.");
        if (string.IsNullOrWhiteSpace(UpdateKey))
            throw new Exception($"Missing required stream setting 'updateKey' for stream '{streamName}'.");
        if (PrimaryKey is null || PrimaryKey.Count == 0)
            throw new Exception($"Missing required stream setting 'primaryKey' for stream '{streamName}'.");

        var chunkSize = string.IsNullOrWhiteSpace(ChunkSize) ? "50000" : ChunkSize.Trim();

        return new ResolvedStreamConfig
        {
            Name = streamName,
            SourceSql = SourceSql,
            TargetTable = TargetTable,
            TargetSchema = string.IsNullOrWhiteSpace(TargetSchema) ? null : TargetSchema,
            PrimaryKey = PrimaryKey,
            ExcludeColumns = ExcludeColumns ?? new List<string>(),
            UpdateKey = UpdateKey,
            ChunkSize = chunkSize,
            StagingFileFormat = string.IsNullOrWhiteSpace(StagingFileFormat) ? "csv.gz" : StagingFileFormat
        };
    }

    private static string? ApplyToken(string? value, string streamName)
    {
        if (string.IsNullOrWhiteSpace(value))
            return value;

        return value.Replace("{stream_table}", streamName, StringComparison.OrdinalIgnoreCase);
    }
}

public sealed class ResolvedStreamConfig
{
    public string Name { get; set; } = "";
    public string SourceSql { get; set; } = "";
    public string TargetTable { get; set; } = "";
    public string? TargetSchema { get; set; }
    public List<string> PrimaryKey { get; set; } = new();
    public List<string> ExcludeColumns { get; set; } = new();
    public string UpdateKey { get; set; } = "";
    public string ChunkSize { get; set; } = "50000";
    public string StagingFileFormat { get; set; } = "csv.gz";
}

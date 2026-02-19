using YamlDotNet.Serialization;

namespace FabricIncrementalReplicator.Config;

public sealed class StreamsConfig
{
    [YamlMember(Alias = "maxParallelStreams")]
    public int? MaxParallelStreams { get; set; }
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

    public int GetMaxParallelStreams()
    {
        var max = MaxParallelStreams ?? 1;
        if (max <= 0)
            throw new Exception($"Invalid streams setting 'maxParallelStreams'={max}. Value must be >= 1.");

        return max;
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
    // Optional update-key interval size (not row count), e.g. "50000", "7d", "2m".
    // If omitted, all data after watermark is processed in one chunk.
    public string? ChunkSize { get; set; }
    // Supported values: "csv.gz" (default), "csv", "parquet"
    public string? StagingFileFormat { get; set; }
    [YamlMember(Alias = "delete_detection")]
    public DeleteDetectionConfig? DeleteDetection { get; set; }

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
            StagingFileFormat = streamOverride?.StagingFileFormat ?? defaults.StagingFileFormat,
            DeleteDetection = DeleteDetectionConfig.Merge(defaults.DeleteDetection, streamOverride?.DeleteDetection)
        };

        merged.SourceSql = ApplyToken(merged.SourceSql, streamName);
        merged.TargetTable = ApplyToken(merged.TargetTable, streamName);
        merged.TargetSchema = ApplyToken(merged.TargetSchema, streamName);
        merged.UpdateKey = ApplyToken(merged.UpdateKey, streamName);
        merged.StagingFileFormat = ApplyToken(merged.StagingFileFormat, streamName);
        if (merged.DeleteDetection is not null)
            merged.DeleteDetection.Where = ApplyToken(merged.DeleteDetection.Where, streamName);

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

        var chunkSize = string.IsNullOrWhiteSpace(ChunkSize) ? null : ChunkSize.Trim();

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
            StagingFileFormat = string.IsNullOrWhiteSpace(StagingFileFormat) ? "csv.gz" : StagingFileFormat,
            DeleteDetection = DeleteDetectionConfig.Resolve(DeleteDetection, streamName)
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
    public string? ChunkSize { get; set; }
    public string StagingFileFormat { get; set; } = "csv.gz";
    public DeleteDetectionConfig DeleteDetection { get; set; } = new() { Type = "none" };
}

public sealed class DeleteDetectionConfig
{
    public string? Type { get; set; }
    public string? Where { get; set; }

    public static DeleteDetectionConfig? Merge(DeleteDetectionConfig? defaults, DeleteDetectionConfig? streamOverride)
    {
        if (defaults is null && streamOverride is null)
            return null;

        return new DeleteDetectionConfig
        {
            Type = streamOverride?.Type ?? defaults?.Type,
            Where = streamOverride?.Where ?? defaults?.Where
        };
    }

    public static DeleteDetectionConfig Resolve(DeleteDetectionConfig? cfg, string streamName)
    {
        if (cfg is null || string.IsNullOrWhiteSpace(cfg.Type))
            return new DeleteDetectionConfig { Type = "none" };

        var t = cfg.Type.Trim().ToLowerInvariant();
        if (t is not ("none" or "subset"))
            throw new Exception($"Unsupported delete_detection.type '{cfg.Type}' for stream '{streamName}'. Supported values: none, subset.");

        return new DeleteDetectionConfig
        {
            Type = t,
            Where = string.IsNullOrWhiteSpace(cfg.Where) ? null : cfg.Where
        };
    }
}

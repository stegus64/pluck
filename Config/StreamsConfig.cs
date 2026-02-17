namespace FabricIncrementalReplicator.Config;

public sealed class StreamsConfig
{
    public List<StreamConfig> Streams { get; set; } = new();
}

public sealed class StreamConfig
{
    public string Name { get; set; } = "";
    public string SourceSql { get; set; } = "";
    public string TargetTable { get; set; } = "";
    public string? TargetSchema { get; set; }
    public List<string> PrimaryKey { get; set; } = new();
    public string UpdateKey { get; set; } = "";
    public int ChunkSize { get; set; } = 50000;
}
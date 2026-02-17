using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace FabricIncrementalReplicator.Config;

public sealed class YamlLoader
{
    private readonly IDeserializer _deserializer =
        new DeserializerBuilder()
            .WithNamingConvention(CamelCaseNamingConvention.Instance)
            .IgnoreUnmatchedProperties()
            .Build();

    public T Load<T>(string path)
    {
        var text = File.ReadAllText(path);
        return _deserializer.Deserialize<T>(text) ?? throw new Exception($"Failed to load YAML: {path}");
    }
}
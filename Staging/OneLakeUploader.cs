using Azure.Storage.Files.DataLake;
using FabricIncrementalReplicator.Auth;
using FabricIncrementalReplicator.Config;

namespace FabricIncrementalReplicator.Staging;

public sealed class OneLakeUploader
{
    private readonly OneLakeStagingConfig _cfg;
    private readonly DataLakeServiceClient _svc;

    public OneLakeUploader(OneLakeStagingConfig cfg, TokenProvider tokenProvider)
    {
        _cfg = cfg;
        _svc = new DataLakeServiceClient(new Uri("https://onelake.dfs.fabric.microsoft.com"), tokenProvider.Credential);
    }

    public async Task<string> UploadAsync(string localPath, string relativeFileName)
    {
        // OneLake DFS URL pattern for COPY INTO:
        // https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<lakehouseId>/Files/<filesPath>/<relativeFileName>
        // OneLake is accessible using ADLS-compatible APIs/SDKs. :contentReference[oaicite:7]{index=7}

        var fs = _svc.GetFileSystemClient(_cfg.WorkspaceId);

        var directoryPath = $"{_cfg.LakehouseId}/Files/{_cfg.FilesPath}".TrimEnd('/');
        var dir = fs.GetDirectoryClient(directoryPath);

        var fullPath = $"{directoryPath}/{relativeFileName}".Replace("\\", "/");
        var file = fs.GetFileClient(fullPath);

        await file.CreateIfNotExistsAsync();

        await using var stream = File.OpenRead(localPath);
        await file.AppendAsync(stream, offset: 0);
        await file.FlushAsync(position: stream.Length);

        return $"https://onelake.dfs.fabric.microsoft.com/{_cfg.WorkspaceId}/{_cfg.LakehouseId}/Files/{_cfg.FilesPath}/{relativeFileName}"
            .Replace("\\", "/");
    }

    public async Task TryDeleteAsync(string relativeFileName)
    {
        try
        {
            var fs = _svc.GetFileSystemClient(_cfg.WorkspaceId);
            var fullPath = $"{_cfg.LakehouseId}/Files/{_cfg.FilesPath}/{relativeFileName}".Replace("\\", "/");
            await fs.DeleteFileAsync(fullPath);
        }
        catch
        {
            // best-effort
        }
    }
}
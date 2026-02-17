using Azure.Storage.Files.DataLake;
using FabricIncrementalReplicator.Auth;
using FabricIncrementalReplicator.Config;
using Microsoft.Extensions.Logging;

namespace FabricIncrementalReplicator.Staging;

public sealed class OneLakeUploader
{
    private readonly OneLakeStagingConfig _cfg;
    private readonly DataLakeServiceClient _svc;
    private readonly Microsoft.Extensions.Logging.ILogger<OneLakeUploader> _log;

    public OneLakeUploader(OneLakeStagingConfig cfg, TokenProvider tokenProvider, Microsoft.Extensions.Logging.ILogger<OneLakeUploader>? log = null)
    {
        _cfg = cfg;
        _svc = new DataLakeServiceClient(new Uri("https://onelake.dfs.fabric.microsoft.com"), tokenProvider.Credential);
        _log = log ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<OneLakeUploader>.Instance;
    }

    public async Task TestConnectionAsync(CancellationToken ct = default)
    {
        var fs = _svc.GetFileSystemClient(_cfg.WorkspaceId);
        // Attempt to read filesystem properties to validate workspace access
        await fs.GetPropertiesAsync(cancellationToken: ct);
        // Also check that the lakehouse Files root (/<lakehouseId>/Files) is accessible.
        // NOTE: per quick connectivity test we ignore the configured FilesPath and only test the Files root.
        var directoryPath = $"{_cfg.LakehouseId}/Files".TrimEnd('/');
        var dir = fs.GetDirectoryClient(directoryPath);
        await dir.GetPropertiesAsync(cancellationToken: ct);
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

        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("OneLake Upload: CreateIfNotExists for {Path}", fullPath);
        await file.CreateIfNotExistsAsync();

        await using var stream = File.OpenRead(localPath);
        _log.LogDebug("OneLake Upload: Append start for {Path}", fullPath);
        await file.AppendAsync(stream, offset: 0);
        await file.FlushAsync(position: stream.Length);
        sw.Stop();
        _log.LogDebug("OneLake Upload elapsed: {Elapsed}ms", sw.Elapsed.TotalMilliseconds);

        return $"https://onelake.dfs.fabric.microsoft.com/{_cfg.WorkspaceId}/{_cfg.LakehouseId}/Files/{_cfg.FilesPath}/{relativeFileName}"
            .Replace("\\", "/");
    }

    public async Task TryDeleteAsync(string relativeFileName)
    {
        try
        {
            var fs = _svc.GetFileSystemClient(_cfg.WorkspaceId);
            var fullPath = $"{_cfg.LakehouseId}/Files/{_cfg.FilesPath}/{relativeFileName}".Replace("\\", "/");
            _log.LogDebug("OneLake Delete attempt: {Path}", fullPath);
            await fs.DeleteFileAsync(fullPath);
        }
        catch
        {
            // best-effort
        }
    }
}
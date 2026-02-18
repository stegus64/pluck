using Azure.Storage.Files.DataLake;
using FabricIncrementalReplicator.Auth;
using FabricIncrementalReplicator.Config;
using Microsoft.Extensions.Logging;

namespace FabricIncrementalReplicator.Staging;

public sealed class OneLakeUploader
{
    private readonly OneLakeStagingConfig _cfg;
    private readonly DataLakeServiceClient _svc;
    private readonly Microsoft.Extensions.Logging.ILogger _log;
    private readonly string _fileSystemName;
    private readonly string _baseDirectoryPath;
    private readonly string _stagingBaseUrl;
    private readonly string _stagingQuery;

    public OneLakeUploader(OneLakeStagingConfig cfg, TokenProvider tokenProvider, Microsoft.Extensions.Logging.ILogger? log = null)
    {
        _cfg = cfg;
        _log = log ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

        var resolved = ResolveStagingTarget(_cfg);
        _svc = new DataLakeServiceClient(resolved.ServiceUrl, tokenProvider.Credential);
        _fileSystemName = resolved.FileSystemName;
        _baseDirectoryPath = resolved.BaseDirectoryPath;
        _stagingBaseUrl = resolved.StagingBaseUrl;
        _stagingQuery = resolved.StagingQuery;
    }

    public async Task TestConnectionAsync(CancellationToken ct = default)
    {
        var fs = _svc.GetFileSystemClient(_fileSystemName);
        // Attempt to read filesystem properties to validate filesystem access.
        await fs.GetPropertiesAsync(cancellationToken: ct);

        if (!string.IsNullOrWhiteSpace(_baseDirectoryPath))
        {
            // Path might not exist yet, but this validates path traversal permission.
            var dir = fs.GetDirectoryClient(_baseDirectoryPath);
            await dir.ExistsAsync(cancellationToken: ct);
        }
    }

    public async Task<string> UploadAsync(string localPath, string relativeFileName)
    {
        var fs = _svc.GetFileSystemClient(_fileSystemName);
        await EnsureDirectoryPathExistsAsync(fs, _baseDirectoryPath);

        var fullPath = CombinePath(_baseDirectoryPath, relativeFileName);
        var file = fs.GetFileClient(fullPath);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        _log.LogDebug("Staging upload: CreateIfNotExists for {Path}", fullPath);
        await file.CreateIfNotExistsAsync();

        await using var stream = File.OpenRead(localPath);
        _log.LogDebug("Staging upload: Append start for {Path}", fullPath);
        await file.AppendAsync(stream, offset: 0);
        await file.FlushAsync(position: stream.Length);
        sw.Stop();
        _log.LogDebug("Staging upload elapsed: {Elapsed}ms", sw.Elapsed.TotalMilliseconds);

        return BuildPublicUrl(_stagingBaseUrl, relativeFileName, _stagingQuery);
    }

    public async Task TryDeleteAsync(string relativeFileName)
    {
        try
        {
            var fs = _svc.GetFileSystemClient(_fileSystemName);
            var fullPath = CombinePath(_baseDirectoryPath, relativeFileName);
            _log.LogDebug("Staging delete attempt: {Path}", fullPath);
            await fs.DeleteFileAsync(fullPath);
        }
        catch
        {
            // best-effort
        }
    }

    private static (Uri ServiceUrl, string FileSystemName, string BaseDirectoryPath, string StagingBaseUrl, string StagingQuery) ResolveStagingTarget(OneLakeStagingConfig cfg)
    {
        if (!string.IsNullOrWhiteSpace(cfg.DataLakeUrl))
            return ParseDataLakeUrl(cfg.DataLakeUrl!);

        if (string.IsNullOrWhiteSpace(cfg.WorkspaceId))
            throw new InvalidOperationException("Missing oneLakeStaging.workspaceId (or set oneLakeStaging.dataLakeUrl).");

        if (string.IsNullOrWhiteSpace(cfg.LakehouseId))
            throw new InvalidOperationException("Missing oneLakeStaging.lakehouseId (or set oneLakeStaging.dataLakeUrl).");

        var serviceUrl = new Uri("https://onelake.dfs.fabric.microsoft.com");
        var basePath = $"{cfg.LakehouseId}/Files/{cfg.FilesPath}".Trim('/');
        var fullBaseUrl = $"{serviceUrl.AbsoluteUri.TrimEnd('/')}/{cfg.WorkspaceId}/{basePath}";
        return (serviceUrl, cfg.WorkspaceId, basePath, fullBaseUrl, string.Empty);
    }

    private static (Uri ServiceUrl, string FileSystemName, string BaseDirectoryPath, string StagingBaseUrl, string StagingQuery) ParseDataLakeUrl(string dataLakeUrl)
    {
        if (!Uri.TryCreate(dataLakeUrl, UriKind.Absolute, out var uri))
            throw new InvalidOperationException($"Invalid oneLakeStaging.dataLakeUrl: '{dataLakeUrl}'.");

        var pathSegments = uri.AbsolutePath.Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (pathSegments.Length == 0)
            throw new InvalidOperationException("oneLakeStaging.dataLakeUrl must include a filesystem/container segment in the path.");

        var serviceUrl = new Uri($"{uri.Scheme}://{uri.Authority}");
        var fileSystemName = pathSegments[0];
        var basePath = string.Join("/", pathSegments.Skip(1));
        var stagingBaseUrl = $"{serviceUrl.AbsoluteUri.TrimEnd('/')}/{fileSystemName}/{basePath}".TrimEnd('/');
        return (serviceUrl, fileSystemName, basePath, stagingBaseUrl, uri.Query ?? string.Empty);
    }

    private static async Task EnsureDirectoryPathExistsAsync(Azure.Storage.Files.DataLake.DataLakeFileSystemClient fs, string baseDirectoryPath)
    {
        if (string.IsNullOrWhiteSpace(baseDirectoryPath))
            return;

        var segments = baseDirectoryPath.Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries);
        var current = "";

        foreach (var segment in segments)
        {
            current = string.IsNullOrEmpty(current) ? segment : $"{current}/{segment}";
            var dir = fs.GetDirectoryClient(current);
            await dir.CreateIfNotExistsAsync();
        }
    }

    private static string CombinePath(string left, string right)
    {
        var leftNorm = (left ?? string.Empty).Trim('/');
        var rightNorm = (right ?? string.Empty).Trim('/');
        if (string.IsNullOrWhiteSpace(leftNorm))
            return rightNorm.Replace("\\", "/");

        return $"{leftNorm}/{rightNorm}".Replace("\\", "/");
    }

    private static string BuildPublicUrl(string baseUrl, string relativePath, string query)
    {
        var path = $"{baseUrl.TrimEnd('/')}/{(relativePath ?? string.Empty).Trim('/')}".Replace("\\", "/");
        if (string.IsNullOrWhiteSpace(query))
            return path;

        return $"{path}{query}";
    }
}

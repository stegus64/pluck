using FabricIncrementalReplicator.Auth;
using FabricIncrementalReplicator.Config;
using Microsoft.Data.SqlClient;

namespace FabricIncrementalReplicator.Target;

public sealed class WarehouseConnectionFactory
{
    private readonly FabricWarehouseConfig _cfg;
    private readonly TokenProvider _tokenProvider;

    public WarehouseConnectionFactory(FabricWarehouseConfig cfg, TokenProvider tokenProvider)
    {
        _cfg = cfg;
        _tokenProvider = tokenProvider;
    }

    public async Task<SqlConnection> OpenAsync(CancellationToken ct = default)
    {
        var cs = new SqlConnectionStringBuilder
        {
            DataSource = _cfg.Server,
            InitialCatalog = _cfg.Database,
            Encrypt = true,
            TrustServerCertificate = false
        }.ConnectionString;

        var conn = new SqlConnection(cs);

        // Fabric Warehouse uses Entra ID; use AAD token for "https://database.windows.net/.default"
        var token = await _tokenProvider.GetTokenAsync(new[] { "https://database.windows.net/.default" }, ct);
        conn.AccessToken = token;

        await conn.OpenAsync(ct);
        return conn;
    }
}
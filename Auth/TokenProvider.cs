using Azure.Core;
using Azure.Identity;
using FabricIncrementalReplicator.Config;

namespace FabricIncrementalReplicator.Auth;

public sealed class TokenProvider
{
    private readonly ChainedTokenCredential _credential;

    public TokenProvider(AuthConfig cfg)
    {
        // Use ClientSecretCredential for service principal (sp) authentication
        var spCredential = new ClientSecretCredential(
            cfg.TenantId,
            cfg.ClientId,
            cfg.ClientSecret
        );

        // Chain with DefaultAzureCredential for local development fallback
        _credential = new ChainedTokenCredential(spCredential, new DefaultAzureCredential());
    }

    public TokenCredential Credential => _credential;

    public async Task<string> GetTokenAsync(string[] scopes, CancellationToken ct = default)
    {
        var token = await _credential.GetTokenAsync(
            new TokenRequestContext(scopes),
            ct
        );
        return token.Token;
    }
}


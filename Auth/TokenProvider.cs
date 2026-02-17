using Azure.Core;
using Azure.Identity;
using FabricIncrementalReplicator.Config;

namespace FabricIncrementalReplicator.Auth;

public sealed class TokenProvider
{
    private readonly TokenCredential _cred;
    private readonly string _tenantId;

    public TokenProvider(AuthConfig auth)
    {
        _tenantId = auth.TenantId;
        _cred = new ClientSecretCredential(auth.TenantId, auth.ClientId, auth.ClientSecret);
    }

    public async Task<string> GetTokenAsync(string[] scopes, CancellationToken ct = default)
    {
        var token = await _cred.GetTokenAsync(new TokenRequestContext(scopes), ct);
        return token.Token;
    }

    public TokenCredential Credential => _cred;
}
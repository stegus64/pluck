using Microsoft.Data.SqlClient;
using System.Data.Common;

namespace Pluck.Source;

public static class SourceProvider
{
    public const string SqlServer = "sqlServer";
    public const string BigQuery = "bigQuery";

    public static string NormalizeSourceType(string? value)
    {
        var normalized = (value ?? SqlServer).Trim().ToLowerInvariant();
        return normalized switch
        {
            "sqlserver" => SqlServer,
            "sql_server" => SqlServer,
            "sql" => SqlServer,
            "bigquery" => BigQuery,
            "big_query" => BigQuery,
            "googlebigquery" => BigQuery,
            "google_bigquery" => BigQuery,
            "bq" => BigQuery,
            _ => value ?? SqlServer
        };
    }

    public static bool SupportsChangeTracking(string sourceType)
    {
        return string.Equals(sourceType, SqlServer, StringComparison.OrdinalIgnoreCase);
    }

    public static DbConnection CreateConnection(string sourceType, string connectionString)
    {
        var normalized = NormalizeSourceType(sourceType);
        if (string.Equals(normalized, SqlServer, StringComparison.OrdinalIgnoreCase))
            return new SqlConnection(connectionString);

        if (string.Equals(normalized, BigQuery, StringComparison.OrdinalIgnoreCase))
        {
            var odbcConnectionType = Type.GetType("System.Data.Odbc.OdbcConnection, System.Data.Odbc", throwOnError: false);
            if (odbcConnectionType is null)
            {
                throw new Exception(
                    "BigQuery source type requires System.Data.Odbc at runtime, but it was not found.");
            }

            var conn = Activator.CreateInstance(odbcConnectionType) as DbConnection;
            if (conn is null)
                throw new Exception("Could not create ODBC source connection.");
            conn.ConnectionString = connectionString;
            return conn;
        }

        throw new Exception(
            $"Unsupported source type '{sourceType}'. Supported values: sqlServer, bigQuery.");
    }

    public static string QuoteIdentifier(string sourceType, string identifier)
    {
        var normalized = NormalizeSourceType(sourceType);
        if (normalized == BigQuery)
            return $"`{identifier.Replace("`", "``", StringComparison.Ordinal)}`";

        return $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}]";
    }

    public static bool UsesPositionalParameters(string sourceType)
    {
        return string.Equals(NormalizeSourceType(sourceType), BigQuery, StringComparison.OrdinalIgnoreCase);
    }
}

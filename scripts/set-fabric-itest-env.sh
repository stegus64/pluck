#!/usr/bin/env bash
set -euo pipefail

# Enable Fabric-backed integration test path.
export PLUCK_ITEST_USE_FABRIC_WAREHOUSE=true

# Optional: choose a non-default connections file/env used by tests.
# Defaults are:
#   PLUCK_ITEST_CONNECTIONS_FILE=connections.yaml
#   PLUCK_ITEST_ENV=dev
export PLUCK_ITEST_CONNECTIONS_FILE="connections.yaml"
export PLUCK_ITEST_ENV="dev"

# Optional per-run overrides (uncomment only if needed):
# export PLUCK_ITEST_FABRIC_SERVER="your-warehouse-host.datawarehouse.fabric.microsoft.com"
# export PLUCK_ITEST_FABRIC_DATABASE="your_warehouse_db"
# export PLUCK_ITEST_FABRIC_TARGET_SCHEMA="dbo"
# export PLUCK_ITEST_FABRIC_TENANT_ID="00000000-0000-0000-0000-000000000000"
# export PLUCK_ITEST_FABRIC_CLIENT_ID="00000000-0000-0000-0000-000000000000"
# export PLUCK_ITEST_FABRIC_CLIENT_SECRET="replace-with-client-secret"
# export PLUCK_ITEST_FABRIC_DATALAKE_URL="https://onelake.dfs.fabric.microsoft.com/<workspace-id>/<lakehouse-id>/Files/staging/incremental-repl/itest"
# export PLUCK_ITEST_FABRIC_WORKSPACE_ID="<workspace-id>"
# export PLUCK_ITEST_FABRIC_LAKEHOUSE_ID="<lakehouse-id>"
# export PLUCK_ITEST_FABRIC_FILES_PATH="staging/incremental-repl/itest"

echo "Fabric integration test environment variables exported."
echo "Run tests with:"
echo "  dotnet test Pluck.Tests/Pluck.Tests.csproj --filter \"FullyQualifiedName~ChangeTracking_Apply_To_Fabric_Target_Should_SoftDelete_Real_Deletes\""

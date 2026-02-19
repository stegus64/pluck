set shell := ["bash", "-cu"]

default:
  @just --list

build:
  dotnet build pluck.sln

test:
  dotnet test pluck.sln

test-unit:
  dotnet test pluck.sln --filter "Category!=Integration"

test-integration:
  dotnet test pluck.sln --filter "Category=Integration"

test-integration-fabric:
  dotnet test Pluck.Tests/Pluck.Tests.csproj --filter "FullyQualifiedName~ChangeTracking_Apply_To_Fabric_Target_Should_SoftDelete_Real_Deletes"

publish-windows:
  dotnet publish pluck.csproj -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true -o publish/win-x64

run *args:
  dotnet run --project pluck.csproj -- {{args}}

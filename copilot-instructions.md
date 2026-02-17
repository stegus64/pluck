# Copilot Instructions for the mysling project

Purpose
- Provide quick, actionable guidance to Copilot/GitHub Copilot when editing this repository.

Purpose of this project
- A data ingestion and warehousing tool that reads from various sources, stages data in CSV format, and loads it into a target warehouse. It is designed to be flexible and extensible, allowing users to define their own sources, staging formats, and target schemas.
- The functionality is inspired by slingdata.io but implemented in C# with a focus on simplicity and ease of use.
- Currently we only support a single sql server as the source, and a single fabric wahrehouse as the target, but the architecture is designed to allow adding more sources and targets in the future.

Original prompt that generated this project
  I want to create a C# program that can be used for incremental replication of a sql server table to a corresponding table in a fabric warehouse. If anything in the specification is unclear, ask questions before generating a large amount of code. The program should be configured using yaml files. One yaml file should contain all connection related information. another yaml file should contain information about the streams to replicate. for each stream we should have the following configurations: stream name source sql target table name primary key update key This key is assumed to be modified every time a row is inserted or updated in the source table chunk size The program shall implement the following pseudo code: foreach stream: Find the definition of the source table from the source database create target table if it does not exist If there are any columns in the source table that does not exist in the target table: add the new columns to the target table. find the maximum value of the update key from the target table find the min and max values of the update key in the source table split the range from of update keys into a number of chunks foreach chunk: Read data for the chunk from the source create a csv.gz file with the chunk data upload the csv.gz file to a temporary folder in onelake create a temporary table in the target database use COPY INTO to read data from the temporary folder to the temporary table Use MERGE to UPSERT data in the target table based on the specified primary key

Project overview
- Language: C# targeting .NET 8.0.
- Solution: mysling.sln (root). Main executable: `program.cs`.
- Key folders and responsibilities:
  - `Auth/`: authentication helpers (e.g., `TokenProvider.cs`).
  - `Config/`: configuration loaders and types (`YamlLoader.cs`, `ConnectionsConfig.cs`, `StreamsConfig.cs`).
  - `Source/`: readers for incoming source definitions.
  - `Staging/`: writers and upload helpers (CSV + OneLake uploader).
  - `Target/`: warehouse loading and schema management.
  - `Util/`: general utilities (type mapping, SQL name helpers, schema validation).

Build & run
- Build: `dotnet build` from repo root.
- Run: `dotnet run --project mysling.csproj` or use the produced executable in `bin/Debug/net8.0/`.

Guidelines for Copilot
- Keep changes minimal and focused: modify only the files required to implement the requested feature or fix.
- Follow existing C# style in the repo: prefer expressive, typed code and avoid unnecessary refactors.
- When adding public APIs, update related config or loader classes in `Config/` and add small unit-style checks if applicable.
- Use apply_patch semantics when creating or editing files: small, atomic patches that preserve surrounding code style.
- Do not add new top-level frameworks or dependencies without explicit user approval.

Testing & verification
- There are no formal test projects in the repo. After changes, run `dotnet build` and optionally run any small example code paths by running the app.

Editing & PR suggestions
- If a change is non-trivial, include a short note at the top of the change describing the intent and any manual verification steps.
- Update this file when repository layout or build steps change.

Where to look for common tasks
- Configuration and YAML handling: `Config/YamlLoader.cs` and related config types.
- Auth/token work: `Auth/TokenProvider.cs`.
- Data ingestion: `Source/*` and `Staging/*`.
- Warehouse interactions: `Target/*`.

If unsure
- Prefer asking the user before making broad changes. Add a short TODO in the code and mention it in the PR description.

Contact
- Leave a concise summary of changes and recommended next steps in the PR body.

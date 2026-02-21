
Goal
Build a C# (.NET) console application named "pluck" implementing a high-level ELT Extract+Load tool with an extensible architecture. V1 must support SQL Server source -> SQL Server target using SqlBulkCopy. Design the code so V2 can add SQL Server -> Fabric Warehouse without major refactors.

Core Requirements (Architecture)
1) Streams
- The program runs one or more "streams" defined in YAML config.
- Streams may run in parallel (configurable max concurrency).
- Each stream loads data in chunks based on an update_key interval; chunk writes must occur in order.
- Processing may be pipelined, but chunk commits to target MUST be in chunk-id order.

2) Chunking and Watermarking (Default incremental mode)
- Each stream declares:
  - update_key (column name)
  - update_key_type: numeric or datetime
  - chunk_size:
    - numeric: a number (e.g., 1000)
    - datetime: a duration string like 2d, 1h, 15m, 1y (calendar add for y, months if supported; keep it simple and implement y/d/h/m/s as needed)
- Algorithm (use this pseudocode semantics):
  foreach stream:
    find watermark by looking at the highest update_key in the target
    Find the range (minkey, maxkey) of update_key in the source > watermark
    lower_bound = minkey
    while lower_bound <= maxkey:
      load chunk [lower_bound, lower_bound + chunk_size)   // half-open interval
      lower_bound += chunk_size
- If target is empty, watermark = null -> full load of all source rows (range from min to max).

3) Transfer and Update Strategies (V1 scope)
- Implement target update strategies:
  - replace_all: truncate target table then load all eligible rows
  - append: insert new rows only (still determined by watermark/filter)
  - upsert is NOT required in V1, but architecture must allow adding it.
- V1 transfer method:
  - SQL Server -> SQL Server via SqlBulkCopy.
- Chunk load: extract chunk rows from source via SqlConnection/SqlCommand, stream into SqlBulkCopy to target.
- Writes must be ordered by chunk id. You may pipeline extraction and preparation, but only one chunk should be committing at a time for a given stream.

4) SQL Server Change Tracking (Design-only for V1, implement minimal scaffolding)
- The architecture MUST include an IncrementalMode concept:
  - watermark (default; implemented in V1)
  - sqlserver_change_tracking (CT; not fully implemented in V1)
- For CT mode:
  - No chunking for delta runs.
  - If first CT run for a table (no CT state), fall back to initial full load using normal chunk-based algorithm, then initialize CT state.
- Implement the interfaces and state store scaffolding so CT can be added in V2/V3 with minimal disruption, but do NOT implement full CT delta logic unless it is small and AGENTS.md allows.

5) No dynamic plugin discovery
- Do not implement runtime assembly scanning or plugin loading.
- Use compiled-in connector registrations (factories/DI) based on YAML "type" fields.

6) Configuration
- YAML-based configuration (single file path passed on CLI).
- Provide a minimal schema and model classes for:
  - defaults (maxConcurrentStreams, chunk parallelism if you implement it, connection references)
  - connections (at least sqlserver source + target)
  - streams:
    - name
    - source: { type: sqlserver, connection: <name>, query or table }
    - target: { type: sqlserver, connection: <name>, table }
    - incremental: { mode: watermark|sqlserver_change_tracking, update_key, update_key_type, chunk_size }
    - update_strategy: replace_all|append
- Include an example YAML in the repo and document how to run.

7) Planning vs Execution
- Implement a planning phase that:
  - loads config
  - validates compatibility and required fields
  - resolves connectors via factories
  - builds an ExecutionPlan per stream: ordered list of ChunkSpecs (ChunkId, Lower, Upper) for watermark mode
- Execution phase runs the plan.

8) Observability
- Structured logging per stream and per chunk.
- At end, print a concise summary (per stream: rows loaded, chunks, duration, status).
- Ensure cancellation (Ctrl+C) triggers graceful stop.

Implementation Guidance (keep code reasonable, not huge)
- Use .NET Generic Host (if allowed by AGENTS.md) for DI and logging.
- Use bounded channels (System.Threading.Channels) or TPL Dataflow (only if allowed) to implement optional pipelining; simplest acceptable is sequential extraction+load per chunk, but stream-level concurrency must exist.
- Use half-open intervals [lower, upper) to avoid duplicates.
- For datetime chunk size parsing: implement parsing for suffixes:
  - s, m, h, d, y (year = AddYears)
- Handle SQL parameterization safely for update_key bounds.

Key Interfaces (must exist)
Define small interfaces to keep extensibility:
- ISourceConnector (create reader)
- IRangedSourceReader
  - GetSourceRangeAsync(watermark) -> (min, max)
  - ReadChunkAsync(lower, upper) -> DbDataReader (streaming) or IDataReader-like
- ITargetConnector
  - GetWatermarkAsync(updateKeyColumn) -> UpdateKey?
  - PrepareAsync(strategy)
  - WriteChunkAsync(dataReader) (V1: SqlBulkCopy)
  - CommitAsync / CompleteAsync
- IIncrementalPlanner
  - BuildPlanAsync(streamConfig, sourceReader, target) -> List<ChunkSpec> or a plan object
- IIncrementalStateStore scaffolding for CT versions (can be no-op in V1, but implement SQL-backed option if easy)

V1 Functional Requirements (must work end-to-end)
- Connect to a SQL Server source, run a query or table extraction with range predicate on update_key.
- Determine watermark from target max(update_key).
- Determine source min/max of update_key where update_key > watermark.
- Generate chunk list based on chunk_size interval.
- For each chunk in order:
  - extract rows for that chunk
  - load via SqlBulkCopy to target
- Streams run concurrently (at least 2 streams can load in parallel).

V2 Prep (do not implement fully unless trivial)
- Add placeholder TargetConnector for Fabric Warehouse and a TransferMethod abstraction suitable for:
  - staging files + COPY/SQL
- Keep this as a TODO with clear extension points, but don’t block V1.

Deliverables
- Working `pluck` console app.
- Example YAML config(s) for V1.
- README instructions.
- Minimal tests (unit tests for interval parsing + chunk plan generation). Use the test framework dictated by AGENTS.md.

Constraints
- Do not generate massive code. Keep it clean, modular, and aligned with the above architecture.
- Prefer correctness and clarity over premature optimization.
- Follow AGENTS.md, repository patterns, and existing tooling.

Start by:
2) Creating the project structure for pluck.
3) Implementing config loading + validation + plan generation.
4) Implementing SQL Server source and SQL Server target with SqlBulkCopy.
5) Adding concurrency across streams.
6) Adding tests for chunk interval parsing and planning.

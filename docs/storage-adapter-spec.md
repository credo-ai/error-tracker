# Storage Adapter Pattern — Spec & Implementation Plan

## Problem

ErrorTracker writes errors directly to a relational database via Ecto. The persistence logic (`upsert_error!/5`) is inlined in `ErrorTracker.report/3` with no abstraction boundary. This makes it impossible to swap the storage backend without forking the library.

Additionally, the Postgres schema prefix is a single static config value (`Application.get_env(:error_tracker, :prefix)`), preventing a single ErrorTracker instance from serving multiple isolated tenants.

**Use case**: A multi-tenant error tracking consumer. Multiple production systems send error data to SQS. A single consumer server ingests from the queue, isolates each sender into its own Postgres schema, and serves a unified web UI with per-sender views. Production systems don't run a UI or fill up a local database.

**Recommended transport**: Amazon SQS. Cross-account access requires only a queue resource policy + IAM role (no VPC peering). ~$1/month at low-to-moderate volume. Consumer side uses Broadway with BroadwaySQS for concurrent ingestion. See Appendix A for full comparison.

## Design

### 1. Storage Adapter Boundary

`report/3` already normalizes all data into clean structs before calling `upsert_error!/5`:

```
report/3
  ├─ normalize_exception/2     → {kind, reason}
  ├─ Stacktrace.new/1          → %Stacktrace{}
  ├─ Error.new/3               → %Error{} (with fingerprint)
  ├─ context merge + sanitize  → map
  ├─ breadcrumbs               → [String.t()]
  │
  └─ ** adapter boundary here **
     │
     └─ upsert_error!/5        → DB transaction (current behavior)
```

The adapter receives five fully-prepared values — all JSON-serializable, all DB-agnostic at this point.

### 2. New Behaviour: `ErrorTracker.Storage`

```elixir
defmodule ErrorTracker.Storage do
  @callback store(
    error :: ErrorTracker.Error.t(),
    stacktrace :: ErrorTracker.Stacktrace.t(),
    context :: map(),
    breadcrumbs :: list(String.t()),
    reason :: String.t(),
    opts :: keyword()
  ) :: ErrorTracker.Occurrence.t() | :noop
end
```

### 3. Configuration

```elixir
# Default (unchanged behavior):
config :error_tracker,
  storage: ErrorTracker.Storage.Ecto

# SQS transport:
config :error_tracker,
  storage: {MyApp.ErrorSQS, queue_url: "https://sqs.us-east-1.amazonaws.com/123456789/error-tracker"}
```

Follows the same `module` or `{module, opts}` pattern used by Ecto adapters and other Elixir libraries. The `opts` are passed as the sixth argument to `store/6` when present (or `[]` when bare module).

### 4. Runtime Prefix Override (Multi-Tenancy)

Currently, `ErrorTracker.Repo.dispatch/3` reads the Postgres schema prefix from application config — a single global value. This prevents concurrent per-tenant operations.

Add a process-dictionary override, mirroring the existing pattern used by `ErrorTracker.set_context/1`:

```elixir
# In ErrorTracker (public API):
def set_prefix(prefix) when is_binary(prefix) do
  Process.put(:error_tracker_prefix, prefix)
end

def get_prefix do
  Process.get(:error_tracker_prefix)
end
```

```elixir
# In Repo.dispatch/3, change prefix resolution:
defp dispatch(action, args, opts) do
  defaults =
    with_adapter(fn
      :postgres ->
        prefix = Process.get(:error_tracker_prefix) ||
                 Application.get_env(:error_tracker, :prefix, "public")
        [prefix: prefix]
      _ -> []
    end)

  opts_w_defaults = Keyword.merge(defaults, opts)
  apply(repo(), action, args ++ [opts_w_defaults])
end
```

This is safe for concurrent operations — each BEAM process has its own dictionary. Broadway processors, LiveView processes, and Oban workers all run in separate processes.

**Auto-provisioning**: When the consumer encounters a new source, it runs migrations for that prefix:

```elixir
prefix = "error_tracker_#{source}"

unless schema_provisioned?(prefix) do
  ErrorTracker.Migration.up(prefix: prefix)
  mark_provisioned(prefix)  # cache in ETS
end

ErrorTracker.set_prefix(prefix)
ErrorTracker.Storage.Ecto.store(error, stacktrace, context, breadcrumbs, reason, [])
```

### 5. Telemetry Strategy

**Producer side (custom adapter)**: Does NOT emit telemetry events. It has no knowledge of whether an error is new, resolved, or muted.

**Consumer side**: After ingesting from SQS and upserting into its local DB, the consumer emits telemetry events. This happens naturally because the consumer uses the default `ErrorTracker.Storage.Ecto` adapter. Telemetry metadata could include the prefix/source for routing notifications.

### 6. Default Adapter: `ErrorTracker.Storage.Ecto`

Direct extraction of the current `upsert_error!/5` logic. No behavioral changes — the existing test suite validates it.

```elixir
defmodule ErrorTracker.Storage.Ecto do
  @behaviour ErrorTracker.Storage

  @impl true
  def store(error, stacktrace, context, breadcrumbs, reason, _opts \\ []) do
    # Exact current upsert_error!/5 logic, including telemetry
  end
end
```

## Implementation Plan

### Phase 1: Extract Storage Behaviour (zero behavior change)

**Files to create:**
1. `lib/error_tracker/storage.ex` — Behaviour definition with `@callback store/6` and docs
2. `lib/error_tracker/storage/ecto.ex` — Move `upsert_error!/5` body here as `store/6`

**Files to modify:**
3. `lib/error_tracker.ex`:
   - Remove `upsert_error!/5` private function
   - Add `storage/0` private helper that reads config
   - Replace `upsert_error!(...)` call in `report/3` with `storage_mod.store(...)`
   - Keep all normalization, filtering, ignoring logic untouched

**Validation:** All existing tests pass with zero changes. The default `:storage` config is `ErrorTracker.Storage.Ecto`, so behavior is identical.

### Phase 2: Runtime Prefix Override

**Files to modify:**
4. `lib/error_tracker.ex`:
   - Add public `set_prefix/1` and `get_prefix/0` functions
   - Add `@doc` and `@spec` annotations

5. `lib/error_tracker/repo.ex`:
   - Modify `dispatch/3` to check `Process.get(:error_tracker_prefix)` before falling back to application config

**Validation:** All existing tests pass — when no process dictionary key is set, behavior is identical (falls back to application config).

### Phase 3: Multi-Tenant Web UI

**Files to modify:**
6. `lib/error_tracker/web/router.ex`:
   - Accept optional `:prefix` in route options, or support a dynamic `:source` path param
   - Pass prefix through to LiveView session

7. `lib/error_tracker/web/hooks/set_assigns.ex`:
   - On mount, call `ErrorTracker.set_prefix/1` if a prefix is present in the session
   - This ensures all `Repo.*` calls in the LiveView use the correct schema

8. `lib/error_tracker/web/live/dashboard.ex`:
   - Add source/prefix to assigns for display (e.g., showing which sender's errors you're viewing)
   - No query changes needed — `Repo.*` calls already go through `dispatch/3` which will use the process dictionary prefix

9. `lib/error_tracker/web/live/show.ex`:
   - Same as dashboard — prefix flows automatically via process dictionary

**Two UI routing strategies** (consumer chooses):

```elixir
# Option A: Static routes per sender (simple, explicit)
scope "/errors/credo-prod" do
  error_tracker_dashboard "/", prefix: "error_tracker_credo_prod"
end

scope "/errors/credo-staging" do
  error_tracker_dashboard "/", prefix: "error_tracker_credo_staging"
end

# Option B: Dynamic route (no pre-registration needed)
scope "/errors/:source" do
  error_tracker_dashboard "/", prefix: :from_params
end
```

Option B derives the prefix from the URL: `/errors/credo-prod` → schema `error_tracker_credo_prod`. The router macro resolves `:from_params` by reading the `:source` path param and constructing the prefix.

### Phase 4: Documentation & Tests

**Files to create:**
10. `guides/storage-adapters.md` — Guide explaining:
    - How the storage adapter works
    - Configuration examples
    - How to implement a custom adapter (SQS example with `ex_aws_sqs`)
    - Consumer-side ingestion pattern (Broadway + BroadwaySQS)
    - Multi-tenant consumer setup (auto-provisioning, dynamic UI routing)
    - Telemetry considerations

11. `test/error_tracker/storage_test.exs` — Test custom storage dispatch:
    - Verify `store/6` is called with correct args
    - Verify `:noop` return works
    - Verify `{Module, opts}` tuple config passes opts through

12. `test/error_tracker/prefix_test.exs` — Test runtime prefix:
    - `set_prefix/1` and `get_prefix/0` round-trip
    - Repo queries use process dictionary prefix when set
    - Falls back to application config when not set
    - Concurrent processes use independent prefixes

### Detailed File Changes

#### `lib/error_tracker/storage.ex` (new)

```elixir
defmodule ErrorTracker.Storage do
  @moduledoc """
  Behaviour for storing errors and occurrences.

  By default, ErrorTracker stores errors directly in the database using Ecto.
  You can implement this behaviour to send error data to an external service
  instead (e.g., AWS SQS, Kafka, HTTP endpoint).

  ## Configuration

      # Default (writes to local database):
      config :error_tracker,
        storage: ErrorTracker.Storage.Ecto

      # Custom adapter with options:
      config :error_tracker,
        storage: {MyApp.SQSStorage, queue_url: "https://sqs.us-east-1.amazonaws.com/..."}

  ## Telemetry

  Custom storage adapters are not expected to emit ErrorTracker telemetry events.
  If you are forwarding errors to a remote ErrorTracker instance, that instance
  will emit telemetry events when it ingests and stores the errors locally.
  """

  @callback store(
    error :: ErrorTracker.Error.t(),
    stacktrace :: ErrorTracker.Stacktrace.t(),
    context :: map(),
    breadcrumbs :: list(String.t()),
    reason :: String.t(),
    opts :: keyword()
  ) :: ErrorTracker.Occurrence.t() | :noop
end
```

#### `lib/error_tracker/storage/ecto.ex` (new)

Direct extraction of `upsert_error!/5` with its imports (`Ecto.Query`, `ErrorTracker.Repo`, `ErrorTracker.Telemetry`, etc.). Keeps telemetry emission.

#### `lib/error_tracker.ex` (modify)

```elixir
# Add public API for prefix:
@doc """
Sets the Postgres schema prefix for the current process.

This overrides the application-level `:prefix` config for all ErrorTracker
operations in the current process. Useful for multi-tenant setups where
different senders' errors are isolated in separate Postgres schemas.

    ErrorTracker.set_prefix("error_tracker_credo_prod")
"""
@spec set_prefix(String.t()) :: String.t() | nil
def set_prefix(prefix) when is_binary(prefix) do
  Process.put(:error_tracker_prefix, prefix)
end

@doc """
Returns the Postgres schema prefix for the current process, or nil if not set.
"""
@spec get_prefix() :: String.t() | nil
def get_prefix do
  Process.get(:error_tracker_prefix)
end

# In report/3, replace:
#   upsert_error!(error, stacktrace, sanitized_context, breadcrumbs, reason)
# With:
#   {storage_mod, storage_opts} = storage()
#   storage_mod.store(error, stacktrace, sanitized_context, breadcrumbs, reason, storage_opts)

# Add private helper:
defp storage do
  case Application.get_env(:error_tracker, :storage, ErrorTracker.Storage.Ecto) do
    {module, opts} -> {module, opts}
    module -> {module, []}
  end
end

# Remove upsert_error!/5 entirely
```

#### `lib/error_tracker/repo.ex` (modify)

```elixir
# In dispatch/3, change the :postgres branch:
defp dispatch(action, args, opts) do
  repo = repo()

  defaults =
    with_adapter(fn
      :postgres ->
        prefix = Process.get(:error_tracker_prefix) ||
                 Application.get_env(:error_tracker, :prefix, "public")
        [prefix: prefix]
      _ -> []
    end)

  opts_w_defaults = Keyword.merge(defaults, opts)
  apply(repo, action, args ++ [opts_w_defaults])
end
```

### Impact Analysis

| Component | Impact |
|---|---|
| `report/3` | Single-line change at call site |
| `resolve/1`, `unresolve/1`, `mute/1`, `unmute/1` | **No change** — use Ecto Repo, now prefix-aware via process dictionary |
| Pruner plugin | **No change** — uses Repo, now prefix-aware if `set_prefix/1` is called before pruning |
| Web UI | **Phase 3** — LiveView mount sets prefix from session, queries automatically scoped |
| Integrations (Plug, Phoenix, Oban) | **No change** — they call `report/3` |
| Telemetry | Emitted by Ecto adapter; skipped by custom adapters |
| Migrations | **No change** — already accept `prefix:` option |
| Existing tests | **No change** — no process dictionary key set = falls back to config |

### Risks & Mitigations

1. **Breaking change?** No. Default behavior is identical. The `:storage` config key is new and optional. `set_prefix/1` is additive.

2. **Ecto still required as dependency?** Yes. The schemas use `Ecto.Schema` and `Ecto.Changeset` for struct building even when not persisting to a DB. This is fine — Ecto is lightweight without a repo.

3. **Custom adapter errors crash the caller?** Same as today — `report/3` is called in the reporting process. The existing `upsert_error!/5` already uses `insert!` (bang). Document that adapters should handle their own errors or let them crash intentionally.

4. **Process dictionary context lost across adapter boundary?** No — `report/3` reads context and breadcrumbs from the process dictionary *before* calling the adapter. The adapter receives plain data.

5. **Race condition on prefix?** No. Process dictionaries are per-process. Broadway processors, LiveView processes, and Oban workers each run in their own process with independent state.

6. **Schema drift across tenants?** Each prefix runs the same migration set. Auto-provisioning calls `ErrorTracker.Migration.up(prefix: prefix)` which applies all migration versions. New ErrorTracker versions require a migration sweep across all prefixes (see Appendix C for the pattern). A `verify_schema/1` function can detect drift by comparing `information_schema.columns` across prefixes.

7. **Pruner needs to run per-prefix?** Yes. The consumer iterates over all provisioned prefixes, calling `set_prefix/1` + `Pruner.prune_errors/1` for each. See Appendix C for the `MultiTenantPruner` implementation.

## Appendix A: AWS Transport Comparison

For cross-account/cross-VPC error streaming at low-to-moderate volume (~100 errors/min, ~13 GB/month):

| Criteria | SQS | Direct HTTPS | EventBridge | Firehose | Kinesis Streams |
|---|---|---|---|---|---|
| **Monthly cost** | ~$1.32 | ~$0 (if endpoint exists) | ~$5.62 | ~$0.38 | ~$30+ |
| **Cross-account setup** | Simple (queue policy) | Needs network path | Medium (rules + IAM) | Medium (IAM + HTTP spec) | Complex (IAM + KCL) |
| **Reliability** | Excellent (built-in) | You build it | Excellent (built-in) | Excellent (built-in) | Excellent (built-in) |
| **Retry/DLQ** | Built-in | DIY | Built-in | Built-in (S3 backup) | N/A (consumer controls) |
| **VPC config needed** | No | Yes (unless public) | No | No (HTTP endpoint) | No |
| **Latency** | Sub-second | Immediate | Sub-second | Seconds (buffered) | Sub-second |

**Recommendation: SQS** — simplest cross-account setup (one queue policy + one IAM role, zero VPC config), excellent reliability with built-in DLQ, and ~$1/month. Consumer side uses Broadway with BroadwaySQS for idiomatic Elixir concurrent message processing.

## Appendix B: credo-backend Integration Notes

Findings from inspecting `/Users/nate/_Work/credo-backend`:

### Current State
- **ErrorTracker is not yet integrated** — greenfield addition
- **ExAws already a dependency** (`ex_aws` ~2.6, `ex_aws_s3`, `ex_aws_sts`) — adding `ex_aws_sqs` is one line in mix.exs
- **IRSA (IAM Roles for Service Accounts)** already configured for Kubernetes — SQS auth works automatically via the existing credential chain
- **Oban is used** (~2.20) — ErrorTracker's Oban integration will automatically catch job failures
- **Two Ecto repos**: `CredoAI.Repo` (main) and `CredoAIAdmin.Repo` (admin) — both Postgres with multi-tenant schema isolation via `Tenant` library
- **Broadway is NOT a dependency** — only needed on the consumer server, not in credo-backend

### Producer-Side Integration (credo-backend)

```elixir
# mix.exs — add deps
{:error_tracker, "~> 0.x"},
{:ex_aws_sqs, "~> 3.4"}

# config/runtime.exs — configure ErrorTracker with SQS adapter
config :error_tracker,
  otp_app: :credo_ai,
  enabled: true,
  storage: {CredoAI.ErrorTracker.SQSStorage,
    queue_url: System.get_env("ERROR_TRACKER_SQS_URL")},
  # No :repo needed — not writing to local DB
  ignorer: CredoAI.ErrorTracker.Ignorer  # optional: filter noisy errors
```

### SQS Message Format

The producer includes a `source` field in every message so the consumer knows which Postgres schema to target:

```elixir
defmodule CredoAI.ErrorTracker.SQSStorage do
  @behaviour ErrorTracker.Storage

  @impl true
  def store(error, stacktrace, context, breadcrumbs, reason, opts) do
    queue_url = Keyword.fetch!(opts, :queue_url)
    source = Keyword.get(opts, :source, "credo-prod")

    payload = %{
      source: source,
      error: serialize_error(error),
      stacktrace: serialize_stacktrace(stacktrace),
      context: context,
      breadcrumbs: breadcrumbs,
      reason: reason
    }

    message_body = JSON.encode!(payload)

    ExAws.SQS.send_message(queue_url, message_body,
      message_group_id: error.fingerprint,
      message_deduplication_id: "#{error.fingerprint}-#{System.system_time(:nanosecond)}"
    )
    |> ExAws.request!()

    :noop
  end

  defp serialize_error(error) do
    Map.take(error, [:kind, :reason, :source_line, :source_function,
                      :fingerprint, :status, :last_occurrence_at])
  end

  defp serialize_stacktrace(stacktrace) do
    %{lines: Enum.map(stacktrace.lines, fn line ->
      Map.take(line, [:application, :module, :function, :arity, :file, :line])
    end)}
  end
end
```

### What credo-backend does NOT need
- ErrorTracker migrations (no local error DB)
- ErrorTracker web UI routes
- ErrorTracker Pruner plugin
- Broadway (that's consumer-side)

### Consumer Server (separate deployment)

A standalone Phoenix app deployed to the monitoring account/VPC:

```elixir
# Broadway pipeline — one per SQS queue (or one shared queue with source routing)
defmodule ErrorConsumer.Pipeline do
  use Broadway

  alias Broadway.Message

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {BroadwaySQS.Producer,
          queue_url: System.get_env("ERROR_TRACKER_SQS_URL"),
          config: [region: "us-east-1"]}
      ],
      processors: [default: [concurrency: 10]],
      batchers: [default: [batch_size: 50, batch_timeout: 1_000]]
    )
  end

  @impl true
  def handle_message(_, %Message{data: data} = message, _) do
    payload = JSON.decode!(data)
    source = payload["source"]
    prefix = "error_tracker_#{source}"

    # Auto-provision schema on first encounter
    SchemaRegistry.ensure_provisioned(prefix)

    # Set prefix for this process — all Repo calls scoped to this schema
    ErrorTracker.set_prefix(prefix)

    # Reconstruct structs and store via Ecto adapter
    {:ok, stacktrace} = build_stacktrace(payload["stacktrace"])
    {:ok, error} = build_error(payload["error"], stacktrace)

    ErrorTracker.Storage.Ecto.store(
      error,
      stacktrace,
      payload["context"],
      payload["breadcrumbs"],
      payload["reason"],
      []
    )

    message
  end
end
```

```elixir
# Router — dynamic multi-tenant UI
scope "/errors/:source" do
  pipe_through [:browser, :admin_auth]
  error_tracker_dashboard "/", prefix: :from_params
end

# /errors/credo-prod      → schema error_tracker_credo_prod
# /errors/credo-staging   → schema error_tracker_credo_staging
# /errors/other-app       → schema error_tracker_other_app
```

The consumer auto-provisions schemas and serves any sender's errors through a single dynamic route. No configuration changes needed when adding new senders — just point a new producer at the same SQS queue with a different `source` value.

## Appendix C: Consumer Tenant Management

Modeled on the patterns in `credo-backend`'s `CredoAI.Release` module, which manages per-tenant Postgres schemas with idempotent creation, cross-tenant migration sweeps, and schema drift verification.

### Schema Registry

An ETS-backed registry that tracks provisioned prefixes and handles idempotent creation:

```elixir
defmodule ErrorConsumer.SchemaRegistry do
  use GenServer

  @table :error_tracker_schemas

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])

    # Seed from existing Postgres schemas on boot
    for prefix <- discover_existing_prefixes() do
      :ets.insert(@table, {prefix, :provisioned})
    end

    {:ok, %{}}
  end

  @doc """
  Ensures the given prefix has a provisioned schema with up-to-date migrations.
  Idempotent — safe to call on every message.
  """
  def ensure_provisioned(prefix) do
    case :ets.lookup(@table, prefix) do
      [{^prefix, :provisioned}] ->
        :ok

      [] ->
        # Serialize schema creation per-prefix to avoid concurrent CREATE SCHEMA races
        GenServer.call(__MODULE__, {:provision, prefix})
    end
  end

  def handle_call({:provision, prefix}, _from, state) do
    # Double-check after acquiring the lock
    case :ets.lookup(@table, prefix) do
      [{^prefix, :provisioned}] ->
        {:reply, :ok, state}

      [] ->
        ErrorTracker.Migration.up(prefix: prefix)
        :ets.insert(@table, {prefix, :provisioned})
        {:reply, :ok, state}
    end
  end

  @doc """
  Returns all known prefixes.
  """
  def all_prefixes do
    :ets.tab2list(@table) |> Enum.map(fn {prefix, _} -> prefix end)
  end

  # Discover existing error_tracker_* schemas from information_schema
  defp discover_existing_prefixes do
    repo = Application.fetch_env!(:error_tracker, :repo)

    {:ok, result} =
      Ecto.Adapters.SQL.query(repo, """
        SELECT schema_name FROM information_schema.schemata
        WHERE schema_name LIKE 'error_tracker_%'
        ORDER BY schema_name
      """, [])

    Enum.map(result.rows, fn [name] -> name end)
  end
end
```

### Migration Sweep

Run after upgrading ErrorTracker to a new version. Iterates all known prefixes and applies pending migrations — same pattern as `CredoAI.Release.do_migrate/3`:

```elixir
defmodule ErrorConsumer.Release do
  require Logger

  @doc """
  Runs ErrorTracker migrations across all provisioned prefixes.
  Call from a release command or Mix task after upgrading ErrorTracker.
  """
  def migrate_all do
    for prefix <- ErrorConsumer.SchemaRegistry.all_prefixes() do
      Logger.info("Running ErrorTracker migrations for #{prefix}")
      ErrorTracker.Migration.up(prefix: prefix)
    end
  end

  @doc """
  Rolls back the last ErrorTracker migration across all provisioned prefixes.
  """
  def rollback_all do
    for prefix <- ErrorConsumer.SchemaRegistry.all_prefixes() do
      Logger.info("Rolling back ErrorTracker migration for #{prefix}")
      ErrorTracker.Migration.down(prefix: prefix)
    end
  end

  @doc """
  Verifies schema consistency across all ErrorTracker prefixes.
  Detects migration drift (e.g., a prefix that missed an upgrade).

  Compares table/column structure via information_schema, modeled on
  CredoAI.Release.verify_schema/1.
  """
  def verify_schema(opts \\ []) do
    repo = Application.fetch_env!(:error_tracker, :repo)
    prefixes = ErrorConsumer.SchemaRegistry.all_prefixes()
    verbose = Keyword.get(opts, :verbose, false)

    if length(prefixes) < 2 do
      Logger.info("Only #{length(prefixes)} prefix(es) — nothing to compare")
      :ok
    else
      [reference | others] = prefixes

      ref_structure = get_schema_structure(repo, reference)

      discrepancies =
        Enum.flat_map(others, fn prefix ->
          other_structure = get_schema_structure(repo, prefix)
          compare_structures(prefix, ref_structure, other_structure, verbose)
        end)

      if discrepancies == [] do
        Logger.info("All #{length(prefixes)} prefixes have consistent schemas")
        :ok
      else
        Logger.error("Found #{length(discrepancies)} discrepancies")
        {:error, discrepancies}
      end
    end
  end

  defp get_schema_structure(repo, prefix) do
    {:ok, result} =
      Ecto.Adapters.SQL.query(repo, """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name LIKE 'error_tracker_%'
        ORDER BY table_name, ordinal_position
      """, [prefix])

    result.rows
    |> Enum.map(fn [table, column, type] -> {table, column, type} end)
    |> Enum.group_by(fn {table, _, _} -> table end)
  end

  defp compare_structures(prefix, ref, other, verbose) do
    ref_tables = Map.keys(ref) |> MapSet.new()
    other_tables = Map.keys(other) |> MapSet.new()

    missing = MapSet.difference(ref_tables, other_tables) |> MapSet.to_list()
    extra = MapSet.difference(other_tables, ref_tables) |> MapSet.to_list()

    table_issues =
      Enum.map(missing, fn t -> {prefix, :missing_table, t} end) ++
        Enum.map(extra, fn t -> {prefix, :extra_table, t} end)

    column_issues =
      MapSet.intersection(ref_tables, other_tables)
      |> Enum.flat_map(fn table ->
        ref_cols = MapSet.new(ref[table])
        other_cols = MapSet.new(other[table])

        missing_cols = MapSet.difference(ref_cols, other_cols) |> MapSet.to_list()
        extra_cols = MapSet.difference(other_cols, ref_cols) |> MapSet.to_list()

        if verbose and missing_cols == [] and extra_cols == [] do
          Logger.info("  #{prefix}.#{table} OK")
        end

        Enum.map(missing_cols, fn {_, col, type} ->
          {prefix, :missing_column, "#{table}.#{col} (#{type})"}
        end) ++
          Enum.map(extra_cols, fn {_, col, type} ->
            {prefix, :extra_column, "#{table}.#{col} (#{type})"}
          end)
      end)

    table_issues ++ column_issues
  end
end
```

### Multi-Tenant Pruner

Runs the standard ErrorTracker Pruner across all prefixes on a schedule:

```elixir
defmodule ErrorConsumer.MultiTenantPruner do
  use GenServer

  require Logger

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    state = %{
      interval: opts[:interval] || :timer.minutes(30),
      max_age: opts[:max_age] || :timer.hours(24),
      limit: opts[:limit] || 200
    }

    {:ok, schedule(state)}
  end

  def handle_info(:prune, state) do
    for prefix <- ErrorConsumer.SchemaRegistry.all_prefixes() do
      ErrorTracker.set_prefix(prefix)

      case ErrorTracker.Plugins.Pruner.prune_errors(
             limit: state.limit,
             max_age: state.max_age
           ) do
        {:ok, pruned} ->
          if pruned != [],
            do: Logger.info("Pruned #{length(pruned)} errors from #{prefix}")

        error ->
          Logger.error("Pruner failed for #{prefix}: #{inspect(error)}")
      end
    end

    {:noreply, schedule(state)}
  end

  defp schedule(%{interval: interval} = state) do
    Process.send_after(self(), :prune, interval)
    state
  end
end
```

### Consumer Application Supervision Tree

```elixir
defmodule ErrorConsumer.Application do
  use Application

  def start(_type, _args) do
    children = [
      ErrorConsumer.Repo,
      ErrorConsumer.SchemaRegistry,       # Must start before Pipeline
      ErrorConsumer.Pipeline,             # Broadway SQS consumer
      ErrorConsumer.MultiTenantPruner,    # Prunes across all prefixes
      ErrorConsumerWeb.Endpoint           # Phoenix with dynamic ErrorTracker UI
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
```

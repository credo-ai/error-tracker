# Storage Adapters

By default, ErrorTracker stores errors directly in your application's database using Ecto. The storage adapter pattern allows you to replace this behavior with a custom transport — sending error data to an external service like AWS SQS, Kafka, or an HTTP endpoint.

This is useful when you want to offload error tracking from production systems. Instead of running the ErrorTracker web UI and accumulating error data locally, your production app sends errors elsewhere. A separate consumer server ingests the data, stores it in its own database, and serves the dashboard.

## Configuration

The `:storage` config key accepts either a module or a `{module, opts}` tuple:

```elixir
# Default — writes directly to the local database:
config :error_tracker,
  storage: ErrorTracker.Storage.Ecto

# Custom adapter with options:
config :error_tracker,
  storage: {MyApp.SQSStorage, queue_url: "https://sqs.us-east-1.amazonaws.com/..."}
```

When omitted, `ErrorTracker.Storage.Ecto` is used.

## Implementing a Custom Adapter

A storage adapter implements the `ErrorTracker.Storage` behaviour with a single callback:

```elixir
defmodule MyApp.SQSStorage do
  @behaviour ErrorTracker.Storage

  @impl true
  def store(error, stacktrace, context, breadcrumbs, reason, opts) do
    queue_url = Keyword.fetch!(opts, :queue_url)
    source = Keyword.get(opts, :source, "my-app")

    payload = %{
      source: source,
      error: serialize_error(error),
      stacktrace: serialize_stacktrace(stacktrace),
      context: context,
      breadcrumbs: breadcrumbs,
      reason: reason
    }

    ExAws.SQS.send_message(queue_url, JSON.encode!(payload))
    |> ExAws.request!()

    :noop
  end

  defp serialize_error(error) do
    Map.take(error, [
      :kind, :reason, :source_line, :source_function,
      :fingerprint, :status, :last_occurrence_at
    ])
  end

  defp serialize_stacktrace(stacktrace) do
    %{lines: Enum.map(stacktrace.lines, fn line ->
      Map.take(line, [:application, :module, :function, :arity, :file, :line])
    end)}
  end
end
```

The callback receives five fully-prepared values that are all JSON-serializable:

| Argument | Type | Description |
|---|---|---|
| `error` | `%ErrorTracker.Error{}` | Error struct with fingerprint, kind, reason, source info |
| `stacktrace` | `%ErrorTracker.Stacktrace{}` | Embedded schema with stack frames |
| `context` | `map()` | Sanitized context (already filtered by `:filter` if configured) |
| `breadcrumbs` | `[String.t()]` | Breadcrumb trail for the process |
| `reason` | `String.t()` | Exception message |
| `opts` | `keyword()` | Options from the `{module, opts}` config tuple (or `[]`) |

Return `%ErrorTracker.Occurrence{}` if you have one, or `:noop` if the adapter does not produce a local occurrence.

### Error Handling

If your adapter raises, the exception propagates to the caller of `ErrorTracker.report/3`. This is the same behavior as the default Ecto adapter. If you need resilience (e.g., SQS is temporarily unavailable), handle errors within your adapter — catch exceptions, buffer locally, retry with backoff, etc.

### Telemetry

Custom adapters should **not** emit ErrorTracker telemetry events. The adapter has no knowledge of whether an error is new, resolved, or muted. If you forward errors to a remote ErrorTracker instance using `ErrorTracker.Storage.Ecto`, that instance emits telemetry events naturally when it ingests the data.

## Multi-Tenant Consumer

A single consumer server can receive errors from multiple producers, isolating each into its own Postgres schema.

### Runtime Prefix

`ErrorTracker.set_prefix/1` overrides the Postgres schema prefix for the current process:

```elixir
ErrorTracker.set_prefix("error_tracker_credo_prod")
# All subsequent Repo calls in this process use the "error_tracker_credo_prod" schema
```

This is safe for concurrent operations — each BEAM process has its own process dictionary.

### Consumer Pipeline

Using [Broadway](https://github.com/dashbitco/broadway) with [BroadwaySQS](https://github.com/dashbitco/broadway_sqs):

```elixir
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
      processors: [default: [concurrency: 10]]
    )
  end

  @impl true
  def handle_message(_, %Message{data: data} = message, _) do
    payload = JSON.decode!(data)
    prefix = "error_tracker_#{payload["source"]}"

    # Auto-provision schema on first encounter
    ErrorConsumer.SchemaRegistry.ensure_provisioned(prefix)

    # Scope all Repo calls to this tenant's schema
    ErrorTracker.set_prefix(prefix)

    # Reconstruct structs and store
    {:ok, stacktrace} = build_stacktrace(payload["stacktrace"])
    {:ok, error} = build_error(payload["error"], stacktrace)

    ErrorTracker.Storage.Ecto.store(
      error, stacktrace,
      payload["context"], payload["breadcrumbs"], payload["reason"],
      []
    )

    message
  end

  defp build_stacktrace(%{"lines" => lines}) do
    # Convert JSON maps back to the format Stacktrace.new/1 expects
    stack = Enum.map(lines, fn line ->
      {
        Module.concat([line["module"]]),
        String.to_atom(line["function"]),
        line["arity"],
        [file: to_charlist(line["file"]), line: line["line"]]
      }
    end)
    ErrorTracker.Stacktrace.new(stack)
  end

  defp build_error(%{} = attrs, stacktrace) do
    ErrorTracker.Error.new(attrs["kind"], attrs["reason"], stacktrace)
  end
end
```

### Schema Registry

An ETS-backed registry that auto-provisions Postgres schemas on first encounter:

```elixir
defmodule ErrorConsumer.SchemaRegistry do
  use GenServer

  @table :error_tracker_schemas

  def start_link(_opts), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def init(_) do
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])

    # Seed from existing schemas on boot
    for prefix <- discover_existing_prefixes() do
      :ets.insert(@table, {prefix, :provisioned})
    end

    {:ok, %{}}
  end

  def ensure_provisioned(prefix) do
    case :ets.lookup(@table, prefix) do
      [{^prefix, :provisioned}] -> :ok
      [] -> GenServer.call(__MODULE__, {:provision, prefix})
    end
  end

  def all_prefixes do
    :ets.tab2list(@table) |> Enum.map(fn {prefix, _} -> prefix end)
  end

  def handle_call({:provision, prefix}, _from, state) do
    case :ets.lookup(@table, prefix) do
      [{^prefix, :provisioned}] ->
        {:reply, :ok, state}
      [] ->
        ErrorTracker.Migration.up(prefix: prefix)
        :ets.insert(@table, {prefix, :provisioned})
        {:reply, :ok, state}
    end
  end

  defp discover_existing_prefixes do
    repo = Application.fetch_env!(:error_tracker, :repo)
    {:ok, result} = Ecto.Adapters.SQL.query(repo, """
      SELECT schema_name FROM information_schema.schemata
      WHERE schema_name LIKE 'error_tracker_%'
      ORDER BY schema_name
    """, [])
    Enum.map(result.rows, fn [name] -> name end)
  end
end
```

### Multi-Tenant Dashboard

The `error_tracker_dashboard` macro accepts a `prefix:` option to scope the UI to a specific schema:

```elixir
# Static — one dashboard per tenant:
scope "/errors/credo-prod" do
  pipe_through [:browser, :admin_auth]
  error_tracker_dashboard "/", prefix: "error_tracker_credo_prod"
end

scope "/errors/credo-staging" do
  pipe_through [:browser, :admin_auth]
  error_tracker_dashboard "/",
    prefix: "error_tracker_credo_staging",
    as: :error_tracker_staging
end
```

For a dynamic route where the prefix is derived from the URL, use a custom `on_mount` hook:

```elixir
# Router
scope "/errors/:source" do
  pipe_through [:browser, :admin_auth]
  error_tracker_dashboard "/",
    on_mount: [{MyApp.SetErrorTrackerPrefix, :from_params}]
end

# Hook
defmodule MyApp.SetErrorTrackerPrefix do
  def on_mount(:from_params, params, _session, socket) do
    if source = params["source"] do
      ErrorTracker.set_prefix("error_tracker_#{source}")
    end
    {:cont, socket}
  end
end
```

### Migration Sweep

After upgrading ErrorTracker, run migrations across all tenant schemas:

```elixir
defmodule ErrorConsumer.Release do
  def migrate_all do
    for prefix <- ErrorConsumer.SchemaRegistry.all_prefixes() do
      ErrorTracker.Migration.up(prefix: prefix)
    end
  end
end
```

### Multi-Tenant Pruner

The default Pruner plugin operates on a single schema. For multi-tenant setups, iterate over all prefixes:

```elixir
defmodule ErrorConsumer.MultiTenantPruner do
  use GenServer

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

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
      ErrorTracker.Plugins.Pruner.prune_errors(limit: state.limit, max_age: state.max_age)
    end
    {:noreply, schedule(state)}
  end

  defp schedule(%{interval: interval} = state) do
    Process.send_after(self(), :prune, interval)
    state
  end
end
```

## Producer-Side Setup (No Local DB)

When using a custom storage adapter, the producer application does not need:

- ErrorTracker migrations
- A `:repo` config
- The ErrorTracker web UI routes
- The Pruner plugin
- Broadway

The producer only needs:

```elixir
# mix.exs
{:error_tracker, "~> 0.8"},
{:ex_aws_sqs, "~> 3.4"}  # or whatever your transport needs

# config/runtime.exs
config :error_tracker,
  otp_app: :my_app,
  enabled: true,
  storage: {MyApp.SQSStorage,
    queue_url: System.get_env("ERROR_TRACKER_SQS_URL"),
    source: "my-app-prod"}
```

The Plug, Phoenix, and Oban integrations all call `ErrorTracker.report/3` which dispatches to your custom adapter automatically.

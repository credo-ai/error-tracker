defmodule ErrorTracker.Consumer.SchemaRegistry do
  @moduledoc """
  ETS-backed GenServer that tracks and auto-provisions Postgres schemas
  for multi-tenant error ingestion.

  On first encounter of a new source, the registry creates the Postgres
  schema and runs ErrorTracker migrations. Subsequent lookups are served
  from ETS without a GenServer round-trip.

  ## Usage

  Add to your supervision tree:

      children = [
        {ErrorTracker.Consumer.SchemaRegistry, prefix_fn: &my_prefix_fn/1}
      ]

  The `:prefix_fn` option is required. It receives the source identifier
  from the SQS message and must return the Postgres schema name:

      defp my_prefix_fn(source), do: "error_tracker_\#{source}"

  ## Options

    * `:prefix_fn` (required) - `(String.t() -> String.t())` function
      that maps a source identifier to a Postgres schema name.
    * `:name` - GenServer name, defaults to `__MODULE__`.
    * `:table` - ETS table name, defaults to `:error_tracker_schema_registry`.
  """

  use GenServer

  alias Ecto.Adapters.SQL

  require Logger

  @table :error_tracker_schema_registry

  # Client API

  @doc """
  Ensures the schema for the given source exists, provisioning it if needed.

  Returns `{:ok, schema_name}`.
  """
  def ensure_schema(source, name \\ __MODULE__) do
    table = GenServer.call(name, :table_name)

    case :ets.lookup(table, source) do
      [{^source, schema}] -> {:ok, schema}
      [] -> GenServer.call(name, {:ensure_schema, source}, :timer.seconds(30))
    end
  end

  @doc """
  Returns a list of all provisioned schema names.
  """
  def all_schemas(name \\ __MODULE__) do
    table = GenServer.call(name, :table_name)
    table |> :ets.tab2list() |> Enum.map(fn {_source, schema} -> schema end)
  end

  # Server

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl GenServer
  def init(opts) do
    prefix_fn = Keyword.fetch!(opts, :prefix_fn)
    table = Keyword.get(opts, :table, @table)

    ets_table = :ets.new(table, [:set, :public, :named_table, read_concurrency: true])

    # Seed from existing schemas on boot
    for schema <- discover_existing_schemas(prefix_fn) do
      :ets.insert(ets_table, {schema_to_source(schema, prefix_fn), schema})
    end

    {:ok, %{table: ets_table, prefix_fn: prefix_fn}}
  end

  @impl GenServer
  def handle_call(:table_name, _from, state) do
    {:reply, state.table, state}
  end

  def handle_call({:ensure_schema, source}, _from, state) do
    # Double-check after acquiring the serialization point
    case :ets.lookup(state.table, source) do
      [{^source, schema}] ->
        {:reply, {:ok, schema}, state}

      [] ->
        schema = state.prefix_fn.(source)

        provision_schema(schema)
        :ets.insert(state.table, {source, schema})
        Logger.info("ErrorTracker: provisioned schema #{schema} for source #{source}")

        {:reply, {:ok, schema}, state}
    end
  end

  defp provision_schema(schema) do
    repo = Application.fetch_env!(:error_tracker, :repo)

    SQL.query!(repo, "CREATE SCHEMA IF NOT EXISTS \"#{schema}\"", [])
    ErrorTracker.Migration.up(prefix: schema)
  end

  defp discover_existing_schemas(_prefix_fn) do
    repo = Application.fetch_env!(:error_tracker, :repo)

    case SQL.query(
           repo,
           """
           SELECT schema_name FROM information_schema.schemata
           WHERE schema_name LIKE 'error_tracker_%'
           ORDER BY schema_name
           """,
           []
         ) do
      {:ok, result} -> Enum.map(result.rows, fn [name] -> name end)
      {:error, _} -> []
    end
  end

  # Best-effort reverse mapping for seeding. If the prefix_fn doesn't produce
  # a reversible mapping, the seed entries just won't be found via ensure_schema
  # but provisioning will still work correctly.
  defp schema_to_source(schema, _prefix_fn) do
    String.replace_prefix(schema, "error_tracker_", "")
  end
end

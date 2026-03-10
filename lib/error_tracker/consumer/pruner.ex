defmodule ErrorTracker.Consumer.Pruner do
  @moduledoc """
  Multi-tenant pruner that iterates all schemas registered in the
  `SchemaRegistry` and prunes resolved errors in each.

  This is the multi-tenant equivalent of `ErrorTracker.Plugins.Pruner`.
  Use this instead of the standard Pruner when running a consumer server
  that ingests errors from multiple sources into separate schemas.

  ## Usage

  Add to your supervision tree after the SchemaRegistry:

      children = [
        {ErrorTracker.Consumer.SchemaRegistry, prefix_fn: &my_prefix_fn/1},
        {ErrorTracker.Consumer.Pruner, limit: 200, max_age: :timer.hours(24)}
      ]

  ## Options

    * `:limit` - max errors to prune per schema per run. Default: `200`.
    * `:max_age` - milliseconds after which resolved errors are pruned.
      Default: `24` hours.
    * `:interval` - milliseconds between pruning runs. Default: `30` minutes.
    * `:schema_registry` - SchemaRegistry name, defaults to
      `ErrorTracker.Consumer.SchemaRegistry`.
  """

  use GenServer

  alias ErrorTracker.Consumer.SchemaRegistry

  require Logger

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(opts) do
    state = %{
      limit: opts[:limit] || 200,
      max_age: opts[:max_age] || :timer.hours(24),
      interval: opts[:interval] || :timer.minutes(30),
      schema_registry: opts[:schema_registry] || SchemaRegistry
    }

    {:ok, schedule_prune(state)}
  end

  @impl GenServer
  def handle_info(:prune, state) do
    prune_all(state)
    {:noreply, schedule_prune(state)}
  end

  defp prune_all(state) do
    schemas = SchemaRegistry.all_schemas(state.schema_registry)

    for schema <- schemas do
      try do
        ErrorTracker.set_prefix(schema)

        ErrorTracker.Plugins.Pruner.prune_errors(
          limit: state.limit,
          max_age: state.max_age
        )
      rescue
        e ->
          Logger.error("ErrorTracker: pruning failed for schema #{schema}: #{Exception.message(e)}")
      after
        Process.delete(:error_tracker_prefix)
      end
    end
  end

  defp schedule_prune(%{interval: interval} = state) do
    Process.send_after(self(), :prune, interval)
    state
  end
end

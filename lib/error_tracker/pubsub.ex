defmodule ErrorTracker.PubSub do
  @moduledoc """
  Broadcasts error events via Phoenix.PubSub for live dashboard updates.

  When a PubSub server is configured, the dashboard will automatically
  refresh when new errors arrive or existing errors change status.

  ## Configuration

      config :error_tracker,
        pubsub_server: MyApp.PubSub

  If no `:pubsub_server` is configured, broadcasting is a no-op.

  ## Topics

  Events are broadcast on prefix-scoped topics:

    * `"error_tracker:<prefix>"` — all events for a specific tenant prefix
    * `"error_tracker"` — all events (when no prefix is set)

  ## Messages

  Subscribers receive messages in the form:

      {:error_tracker, event_type, %{error: error}}

  Where `event_type` is one of:
    * `:new_error`
    * `:unresolved_error`
    * `:resolved_error`
    * `:new_occurrence`
  """

  @handler_id "error-tracker-pubsub"

  @doc false
  def attach do
    if pubsub_server() do
      :telemetry.attach_many(
        @handler_id,
        [
          [:error_tracker, :error, :new],
          [:error_tracker, :error, :unresolved],
          [:error_tracker, :error, :resolved],
          [:error_tracker, :occurrence, :new]
        ],
        &__MODULE__.handle_event/4,
        %{}
      )
    end
  end

  @doc false
  def detach do
    :telemetry.detach(@handler_id)
  end

  @doc """
  Subscribe to error events for the given prefix.
  """
  def subscribe(prefix \\ nil) do
    if server = pubsub_server() do
      Phoenix.PubSub.subscribe(server, topic(prefix))
    end
  end

  @doc false
  def handle_event([:error_tracker, :error, :new], _measurements, metadata, _config) do
    broadcast(:new_error, metadata)
  end

  def handle_event([:error_tracker, :error, :unresolved], _measurements, metadata, _config) do
    broadcast(:unresolved_error, metadata)
  end

  def handle_event([:error_tracker, :error, :resolved], _measurements, metadata, _config) do
    broadcast(:resolved_error, metadata)
  end

  def handle_event([:error_tracker, :occurrence, :new], _measurements, metadata, _config) do
    broadcast(:new_occurrence, metadata)
  end

  defp broadcast(event_type, metadata) do
    if server = pubsub_server() do
      prefix = ErrorTracker.get_prefix()
      message = {:error_tracker, event_type, Map.take(metadata, [:error, :occurrence, :muted])}

      Phoenix.PubSub.broadcast(server, topic(prefix), message)
    end
  end

  @doc false
  def topic(nil), do: "error_tracker"
  def topic(prefix), do: "error_tracker:#{prefix}"

  defp pubsub_server do
    Application.get_env(:error_tracker, :pubsub_server)
  end
end

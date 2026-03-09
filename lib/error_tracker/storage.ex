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

  When using the `{module, opts}` tuple form, `opts` is passed as the last
  argument to `store/6`.

  ## Telemetry

  The default `ErrorTracker.Storage.Ecto` adapter emits telemetry events after
  storing errors and occurrences. Custom storage adapters are not expected to
  emit these events. If you are forwarding errors to a remote ErrorTracker
  instance, that instance will emit telemetry events when it ingests and stores
  the errors locally.
  """

  @doc """
  Store an error and its occurrence.

  Receives the fully-prepared error struct, stacktrace, sanitized context,
  breadcrumbs, reason string, and any adapter-specific options from the
  configuration.

  Should return the stored `%ErrorTracker.Occurrence{}` or `:noop`.
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

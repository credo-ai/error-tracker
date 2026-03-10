if Code.ensure_loaded?(Broadway) do
  defmodule ErrorTracker.Consumer.Pipeline do
    @moduledoc """
    Broadway pipeline that consumes error messages from SQS and stores them
    using `ErrorTracker.Storage.Ecto`.

    Each message is expected to be a JSON payload produced by a storage adapter
    (like the SQS adapter in a producer application). The pipeline decodes the
    message, provisions the tenant schema if needed, and stores the error.

    ## Usage

    Add to your supervision tree:

        children = [
          {ErrorTracker.Consumer.SchemaRegistry, prefix_fn: &my_prefix_fn/1},
          {ErrorTracker.Consumer.Pipeline,
           queue_url: System.get_env("ERROR_TRACKER_SQS_URL"),
           schema_registry: ErrorTracker.Consumer.SchemaRegistry}
        ]

    ## Options

      * `:queue_url` (required) - SQS queue URL to consume from.
      * `:schema_registry` - SchemaRegistry name, defaults to
        `ErrorTracker.Consumer.SchemaRegistry`.
      * `:processor_concurrency` - number of Broadway processors, defaults to `10`.
      * `:producer_concurrency` - number of SQS receive-message calls, defaults to `1`.
      * `:sqs_config` - keyword list passed to `BroadwaySQS.Producer` as `:config`.
    """

    use Broadway

    alias Broadway.Message
    alias ErrorTracker.Consumer.SchemaRegistry

    require Logger

    def start_link(opts) do
      queue_url = Keyword.fetch!(opts, :queue_url)
      registry = Keyword.get(opts, :schema_registry, SchemaRegistry)
      processor_concurrency = Keyword.get(opts, :processor_concurrency, 10)
      producer_concurrency = Keyword.get(opts, :producer_concurrency, 1)
      sqs_config = Keyword.get(opts, :sqs_config, [])

      Broadway.start_link(__MODULE__,
        name: __MODULE__,
        producer: [
          module: {BroadwaySQS.Producer, queue_url: queue_url, config: sqs_config},
          concurrency: producer_concurrency
        ],
        processors: [
          default: [concurrency: processor_concurrency]
        ],
        context: %{schema_registry: registry}
      )
    end

    @impl Broadway
    def handle_message(_, %Message{data: data} = message, context) do
      payload = decode_payload(data)
      source = Map.fetch!(payload, "source")

      {:ok, schema} = SchemaRegistry.ensure_schema(source, context.schema_registry)

      try do
        ErrorTracker.set_prefix(schema)

        {error, stacktrace} = reconstruct_structs(payload)

        ErrorTracker.Storage.Ecto.store(
          error,
          stacktrace,
          payload["context"] || %{},
          payload["breadcrumbs"] || [],
          payload["reason"] || "",
          []
        )
      after
        Process.delete(:error_tracker_prefix)
      end

      message
    rescue
      e ->
        Logger.error("ErrorTracker consumer failed to process message: #{Exception.message(e)}")

        Message.failed(message, Exception.message(e))
    end

    @impl Broadway
    def handle_failed(messages, _context) do
      for message <- messages do
        Logger.warning("ErrorTracker consumer message failed: #{inspect(message.status)}")
      end

      messages
    end

    defp decode_payload(data) when is_binary(data) do
      json_decode!(data)
    end

    defp decode_payload(%{"Body" => body}) when is_binary(body) do
      json_decode!(body)
    end

    defp json_decode!(data) do
      cond do
        Code.ensure_loaded?(JSON) and function_exported?(JSON, :decode!, 1) ->
          JSON.decode!(data)

        Code.ensure_loaded?(Jason) ->
          Jason.decode!(data)

        true ->
          raise "No JSON library available. Add :jason or use Elixir 1.18+ for built-in JSON."
      end
    end

    defp reconstruct_structs(payload) do
      stacktrace = reconstruct_stacktrace(payload["stacktrace"])
      error = reconstruct_error(payload["error"], stacktrace)
      {error, stacktrace}
    end

    defp reconstruct_stacktrace(%{"lines" => lines}) do
      # Build the raw stacktrace format that Stacktrace.new/1 expects:
      # [{module, function, arity, [file: charlist, line: integer]}]
      raw_stack =
        Enum.map(lines, fn line ->
          {
            Module.concat([line["module"]]),
            String.to_atom(line["function"]),
            line["arity"],
            [file: to_charlist(line["file"] || ""), line: line["line"]]
          }
        end)

      {:ok, stacktrace} = ErrorTracker.Stacktrace.new(raw_stack)
      stacktrace
    end

    defp reconstruct_error(error_map, stacktrace) do
      kind = error_map["kind"]
      reason = error_map["reason"]

      {:ok, error} = ErrorTracker.Error.new(kind, reason, stacktrace)
      error
    end
  end
end

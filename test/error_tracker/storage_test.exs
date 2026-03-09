defmodule ErrorTracker.StorageTest do
  use ErrorTracker.Test.Case

  defmodule TestStorage do
    @moduledoc false
    @behaviour ErrorTracker.Storage

    @impl true
    def store(error, _stacktrace, _context, _breadcrumbs, _reason, opts) do
      send(opts[:test_pid], {:stored, error.fingerprint, opts})
      :noop
    end
  end

  setup do
    previous = Application.get_env(:error_tracker, :storage)

    on_exit(fn ->
      if previous do
        Application.put_env(:error_tracker, :storage, previous)
      else
        Application.delete_env(:error_tracker, :storage)
      end
    end)

    []
  end

  test "uses ErrorTracker.Storage.Ecto by default" do
    Application.delete_env(:error_tracker, :storage)

    assert %ErrorTracker.Occurrence{} =
             report_error(fn -> raise "default adapter" end)
  end

  test "dispatches to a custom storage module" do
    Application.put_env(:error_tracker, :storage, {TestStorage, test_pid: self()})

    assert :noop = report_error(fn -> raise "custom adapter" end)
    assert_receive {:stored, _fingerprint, opts}
    assert opts[:test_pid] == self()
  end

  test "passes opts from {module, opts} config tuple" do
    Application.put_env(
      :error_tracker,
      :storage,
      {TestStorage, test_pid: self(), queue_url: "https://sqs.example.com"}
    )

    report_error(fn -> raise "opts test" end)
    assert_receive {:stored, _fingerprint, opts}
    assert opts[:queue_url] == "https://sqs.example.com"
  end

  test "bare module config passes empty opts" do
    # Use Ecto adapter as a bare module (no tuple)
    Application.put_env(:error_tracker, :storage, ErrorTracker.Storage.Ecto)

    assert %ErrorTracker.Occurrence{} =
             report_error(fn -> raise "bare module" end)
  end
end

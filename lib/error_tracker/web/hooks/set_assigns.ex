defmodule ErrorTracker.Web.Hooks.SetAssigns do
  @moduledoc false

  import Phoenix.Component, only: [assign: 2]

  def on_mount({:set_dashboard_path, path}, params, session, socket) do
    if prefix = session["prefix"] do
      ErrorTracker.set_prefix(prefix)
    end

    resolved_path = resolve_path_params(path, params)
    socket = %{socket | private: Map.put(socket.private, :dashboard_path, resolved_path)}

    {:cont, assign(socket, csp_nonces: session["csp_nonces"])}
  end

  # Replace route params like :source with their actual values from the URL
  defp resolve_path_params(path, params) do
    Enum.reduce(params, path, fn {key, value}, acc ->
      String.replace(acc, ":#{key}", value)
    end)
  end
end

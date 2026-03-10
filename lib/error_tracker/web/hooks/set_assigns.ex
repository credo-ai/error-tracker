defmodule ErrorTracker.Web.Hooks.SetAssigns do
  @moduledoc false

  import Phoenix.Component, only: [assign: 2]

  def on_mount({:set_dashboard_path, path}, _params, session, socket) do
    if prefix = session["prefix"] do
      ErrorTracker.set_prefix(prefix)
    end

    socket = %{socket | private: Map.put(socket.private, :dashboard_path, path)}

    {:cont, assign(socket, csp_nonces: session["csp_nonces"])}
  end
end

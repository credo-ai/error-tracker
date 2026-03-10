defmodule ErrorTracker.Consumer.Release do
  @moduledoc """
  Release tasks for running ErrorTracker migrations across all tenant schemas.

  Use these functions from your application's release module to migrate all
  provisioned schemas at deploy time.

  ## Usage

      defmodule MyApp.Release do
        def migrate_error_tracker do
          # Ensure the app is started (for Repo access)
          Application.ensure_all_started(:my_app)

          # Migrate all known schemas
          ErrorTracker.Consumer.Release.migrate_all()
        end
      end

  Then invoke from a release command:

      bin/my_app eval "MyApp.Release.migrate_error_tracker()"

  ## Pre-provisioning

  If the SchemaRegistry is not running (e.g., during a migration-only release
  task), you can pass an explicit list of schemas:

      ErrorTracker.Consumer.Release.migrate_all(
        schemas: ["error_tracker_app_a", "error_tracker_app_b"]
      )
  """

  alias ErrorTracker.Consumer.SchemaRegistry

  require Logger

  @doc """
  Runs ErrorTracker migrations for all known schemas.

  ## Options

    * `:schemas` - explicit list of schema names. When omitted, reads from
      the SchemaRegistry (which must be running).
    * `:schema_registry` - SchemaRegistry name, defaults to
      `ErrorTracker.Consumer.SchemaRegistry`.
    * `:version` - target migration version. When omitted, migrates to latest.
  """
  def migrate_all(opts \\ []) do
    schemas = schemas_from_opts(opts)
    version_opts = if v = opts[:version], do: [version: v], else: []

    for schema <- schemas do
      migrate_schema(schema, version_opts)
    end

    :ok
  end

  @doc """
  Runs ErrorTracker migrations for a single schema.

  Creates the Postgres schema if it doesn't exist.
  """
  def migrate_schema(schema, opts \\ []) do
    repo = Application.fetch_env!(:error_tracker, :repo)

    Logger.info("ErrorTracker: migrating schema #{schema}")
    Ecto.Adapters.SQL.query!(repo, "CREATE SCHEMA IF NOT EXISTS \"#{schema}\"", [])
    ErrorTracker.Migration.up(Keyword.put(opts, :prefix, schema))
  end

  @doc """
  Rolls back ErrorTracker migrations for a single schema.
  """
  def rollback_schema(schema, opts \\ []) do
    Logger.info("ErrorTracker: rolling back schema #{schema}")
    ErrorTracker.Migration.down(Keyword.put(opts, :prefix, schema))
  end

  defp schemas_from_opts(opts) do
    case Keyword.get(opts, :schemas) do
      nil ->
        registry = Keyword.get(opts, :schema_registry, SchemaRegistry)
        SchemaRegistry.all_schemas(registry)

      schemas when is_list(schemas) ->
        schemas
    end
  end
end

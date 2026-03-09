defmodule ErrorTracker.Storage.Ecto do
  @moduledoc """
  Default storage adapter that persists errors and occurrences to the database
  using Ecto.

  This adapter handles deduplication via fingerprint-based upserts and emits
  telemetry events after successful storage.
  """

  @behaviour ErrorTracker.Storage

  import Ecto.Query

  alias ErrorTracker.Error
  alias ErrorTracker.Occurrence
  alias ErrorTracker.Repo
  alias ErrorTracker.Telemetry

  @impl true
  def store(error, stacktrace, context, breadcrumbs, reason, _opts) do
    status_and_muted_query =
      from e in Error,
        where: [fingerprint: ^error.fingerprint],
        select: {e.status, e.muted}

    {existing_status, muted} =
      case Repo.one(status_and_muted_query) do
        {existing_status, muted} -> {existing_status, muted}
        nil -> {nil, false}
      end

    {:ok, {error, occurrence}} =
      Repo.transaction(fn ->
        error =
          Repo.with_adapter(fn
            :mysql ->
              Repo.insert!(error,
                on_conflict: [set: [status: :unresolved, last_occurrence_at: DateTime.utc_now()]]
              )

            _other ->
              Repo.insert!(error,
                on_conflict: [set: [status: :unresolved, last_occurrence_at: DateTime.utc_now()]],
                conflict_target: :fingerprint
              )
          end)

        occurrence =
          error
          |> Ecto.build_assoc(:occurrences)
          |> Occurrence.changeset(%{
            stacktrace: stacktrace,
            context: context,
            breadcrumbs: breadcrumbs,
            reason: reason
          })
          |> Repo.insert!()

        {error, occurrence}
      end)

    %Occurrence{} = occurrence
    occurrence = %{occurrence | error: error}

    case existing_status do
      :resolved -> Telemetry.unresolved_error(error)
      :unresolved -> :noop
      nil -> Telemetry.new_error(error)
    end

    Telemetry.new_occurrence(occurrence, muted)
    occurrence
  end
end

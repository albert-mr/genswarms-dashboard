# Synthetic local benchmark: MIX_ENV=test mix run scripts/snapshot_scale.exs
# No source API, database or user data is accessed.
defmodule SnapshotScale do
  alias SubzeroSwarmDashboardWeb.{SessionsLive, SnapshotView}

  def run do
    rows =
      for n <- 1..20_000 do
        %{
          "session_id" => "test:#{n}:0",
          "state" => "stored",
          "agent" => nil,
          "transport" => "test",
          "last_activity" => "2020-01-01T00:00:00Z",
          "transport_ref" => %{"chat_id" => "#{n}", "thread_id" => "0"},
          "metadata" => %{"chat_type" => "dm"},
          "user" => %{"handle" => "synthetic_#{n}", "name" => "Person #{n}"}
        }
      end

    source = %{
      "sessions" => rows,
      "extensions" => %{
        "consumers" => %{
          "count" => 20_000,
          "items" => Enum.map(rows, &%{"session_id" => &1["session_id"], "mode" => "scout"})
        }
      }
    }

    decoded = source |> Jason.encode!() |> Jason.decode!()

    {micros, page} =
      :timer.tc(fn -> SnapshotView.project(decoded, %{dashboard_view: SessionsLive}) end)

    unless length(page["sessions"]) == 50 and page["_sessions_page"].total == 20_000,
      do: raise("pagination changed population coverage")

    old = measure(decoded, {:snapshot, decoded})
    new = measure(page, {:snapshot_ready, 123})

    unless new.queued_bytes < old.queued_bytes / 10,
      do: raise("snapshot amplification regression")

    IO.puts(
      Jason.encode!(%{
        records: 20_000,
        rows_per_view: 50,
        projection_ms: micros / 1000,
        old: old,
        projected: new,
        source_json_bytes: byte_size(Jason.encode!(decoded)),
        projected_json_bytes: byte_size(Jason.encode!(page))
      })
    )
  end

  defp measure(snapshot, message) do
    owner = self()

    pid =
      spawn(fn ->
        receive do
          {:hold, value} ->
            :erlang.garbage_collect()
            send(owner, :held)
            hold(value)
        end
      end)

    monitor = Process.monitor(pid)
    send(pid, {:hold, snapshot})

    receive do
      :held -> :ok
    end

    one = memory(pid)
    for _ <- 1..12, do: send(pid, message)
    send(pid, {:barrier, self()})

    receive do
      :barrier -> :ok
    end

    queued = memory(pid)
    send(pid, {:stop, self()})

    receive do
      {:stopped, count} when count > 0 -> :ok
    end

    receive do
      {:DOWN, ^monitor, :process, ^pid, :normal} -> :ok
    end

    %{one_view_bytes: one, queued_bytes: queued}
  end

  defp hold(snapshot) do
    receive do
      {:barrier, owner} ->
        send(owner, :barrier)
        hold(snapshot)

      {:stop, owner} ->
        send(owner, {:stopped, length(snapshot["sessions"])})
    end
  end

  defp memory(pid), do: Process.info(pid, :memory) |> elem(1)
end

SnapshotScale.run()

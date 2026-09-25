defmodule SubzeroSwarmDashboard.SwarmFeed do
  @moduledoc """
  Polls the swarm `/dashboard` aggregate every `poll_interval_ms` while viewers
  are subscribed and republishes snapshots over `Phoenix.PubSub` (topic `"feed"`).
  The Slipstream WS client (`SwarmFeed.Socket`) publishes live `{:event, ...}` on the same topic;
  LiveViews subscribe to `"feed"`.

  Silent-empty guard (spec §5 C1/R3): if a snapshot reports agents but no WS event
  has arrived for a while, broadcast `{:warning, :endpoint_not_colocated}` — the
  classic "API not co-located with the swarm BEAM" failure.

  Messages broadcast on `"feed"`:
    - `{:snapshot, map}` — a fresh `/dashboard` aggregate
    - `{:disconnected, reason}` — the swarm is unreachable
    - `{:event, type, payload}` — a live WS event (from `SwarmFeed.Socket`)
    - `{:warning, :endpoint_not_colocated}`
  """
  use GenServer
  require Logger

  alias Phoenix.PubSub
  alias SubzeroSwarmDashboard.SwarmClient

  @pubsub SubzeroSwarmDashboard.PubSub
  @topic "feed"
  @silent_after_ms 15_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Topic LiveViews subscribe to."
  def topic, do: @topic

  def subscribe do
    :ok = PubSub.subscribe(@pubsub, @topic, metadata: :snapshot_viewer)
    GenServer.cast(__MODULE__, :viewer_joined)
  end

  @doc """
  Last snapshot the poller fetched, or nil before the first successful poll.
  Lets a freshly mounted LiveView render the full menu + page immediately
  instead of flashing the empty state for up to one poll interval (3s).
  Nil-safe when the feed process isn't running (tests, boot races).
  """
  def current do
    GenServer.call(__MODULE__, :current, 1_000)
  catch
    :exit, _ -> nil
  end

  @impl true
  def init(_opts) do
    PubSub.subscribe(@pubsub, @topic)
    interval = Application.get_env(:subzero_swarm_dashboard, :poll_interval_ms, 3_000)
    swarm = Application.get_env(:subzero_swarm_dashboard, :swarm_name, "wingston")

    {:ok,
     %{
       interval: interval,
       timer: :erlang.start_timer(0, self(), :poll),
       idle: true,
       swarm: swarm,
       last_snapshot: nil,
       last_event_at: nil,
       started_at: now_ms()
     }}
  end

  @impl true
  def handle_info({:timeout, timer, :poll}, %{timer: timer} = state) do
    # Native PubSub registration follows LiveView lifetime and survives a feed
    # restart. EventsFeed and our silent guard subscribe without viewer metadata.
    # Keep the cheap demand check alive; continuous event/WS collection runs
    # separately so unseen events and cursor history are not lost.
    viewers? =
      Enum.any?(Registry.lookup(@pubsub, @topic), fn {_, meta} -> meta == :snapshot_viewer end)

    state = %{state | idle: not viewers?}
    state = if viewers?, do: poll(state), else: state
    {:noreply, %{state | timer: :erlang.start_timer(state.interval, self(), :poll)}}
  end

  # Ignore a canceled timer already queued when a viewer wakes the feed.
  def handle_info({:timeout, _, :poll}, state), do: {:noreply, state}

  # Observe live WS events (from the Socket) to feed the silent-empty guard.
  def handle_info({:event, _type, _payload}, state),
    do: {:noreply, %{state | last_event_at: now_ms()}}

  # Ignore our own broadcasts echoed back to us.
  def handle_info({:snapshot, _}, state), do: {:noreply, state}
  def handle_info({:disconnected, _}, state), do: {:noreply, state}
  def handle_info({:warning, _}, state), do: {:noreply, state}
  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def handle_call(:current, _from, state), do: {:reply, state.last_snapshot, state}

  @impl true
  def handle_cast(:viewer_joined, %{idle: true} = state) do
    Process.cancel_timer(state.timer)
    {:noreply, %{state | timer: :erlang.start_timer(0, self(), :poll), idle: false}}
  end

  def handle_cast(:viewer_joined, state), do: {:noreply, state}

  defp poll(state) do
    case SwarmClient.dashboard(state.swarm) do
      {:ok, snap} ->
        PubSub.broadcast(@pubsub, @topic, {:snapshot, snap})
        maybe_warn_silent(snap, state)
        %{state | last_snapshot: snap}

      {:error, reason} ->
        PubSub.broadcast(@pubsub, @topic, {:disconnected, reason})
        state
    end
  end

  defp maybe_warn_silent(snap, state) do
    now = now_ms()

    if warn_silent?(snap, state.last_event_at, now - state.started_at, now, @silent_after_ms) do
      PubSub.broadcast(@pubsub, @topic, {:warning, :endpoint_not_colocated})
    end
  end

  @doc """
  Pure guard decision: warn when the snapshot reports agents, the feed has been
  running past `threshold`, and no WS event has arrived within `threshold`.
  """
  @spec warn_silent?(map(), integer() | nil, integer(), integer(), integer()) :: boolean()
  def warn_silent?(snap, last_event_at, running_ms, now, threshold) do
    agents = get_in(snap, ["summary", "agents"]) || 0
    silent? = is_nil(last_event_at) or now - last_event_at > threshold
    agents > 0 and running_ms > threshold and silent?
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end

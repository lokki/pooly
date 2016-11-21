defmodule Pooly.Server do
  use GenServer

  defmodule State do
    defstruct sup: nil, size: nil, mfa: nil, worker_sup: nil, workers: nil, monitors: nil
  end

  ## Client API

  def start_link(sup, pool_config) do
    GenServer.start_link(__MODULE__, [sup, pool_config], name: __MODULE__)
  end

  def checkout do
    GenServer.call(__MODULE__, :checkout)
  end

  def checkin(worker_pid) do
    GenServer.cast(__MODULE__, {:checkin, worker_pid})
  end

  def status do
    GenServer.call(__MODULE__, :status)
  end

  ## Callbacks

  def init([pid, pool_config]) when is_pid(pid) do
    Process.flag(:trap_exits, true)
    monitors = :ets.new(:monitors, [:set, :private])
    init(pool_config, %State{sup: pid, monitors: monitors})
  end

  def handle_info(:start_worker_supervisor, %State{sup: sup, mfa: mfa, size: size} = state) do
    {:ok, worker_sup} = Supervisor.start_child(sup, supervisor_spec(mfa))
    workers = prepopulate(size, worker_sup)
    {:noreply, %State{state | worker_sup: worker_sup, workers: workers}}
  end

  def handle_info({:DOWN, ref, _, _, _}, %State{workers: workers, monitors: monitors} = state) do
    case :ets.match(monitors, {:"$1", ref}) do
      [[pid]] ->
        true = :ets.delete(monitors, pid)
        new_state = %State{state | workers: [pid | workers]}
        {:noreply, new_state}
      [[]] ->
        {:noreply, state}
    end
  end

  def handle_info({:EXIT, pid, _reason}, %State{workers: workers, monitors: monitors, worker_sup: worker_sup} = state) do
    case :ets.lookup(monitors, pid) do
      [{^pid, ref}] ->
        true = Process.demonitor(ref)
        true = :ets.delete(monitors, pid)
        new_state = %State{state | workers: [new_worker(worker_sup) | workers]}
        {:noreply, new_state}
      _ ->
        {:noreply, state}
    end
  end

  def handle_call(:checkout, {from_pid, _ref}, %State{workers: workers, monitors: monitors} = state) do
    case workers do
      [worker | rest] ->
        ref = Process.monitor(from_pid)
        true = :ets.insert(monitors, {worker, ref})
        {:reply, worker, %State{state | workers: rest}}
      [] -> 
        {:reply, :noproc, state}
    end
  end

  def handle_call(:status, _from, %State{workers: workers, monitors: monitors} = state) do
    {:reply, {length(workers), :ets.info(monitors, :size)}, state}
  end

  def handle_cast({:checkin, worker}, %State{workers: workers, monitors: monitors} = state) when is_pid(worker) do
    case :ets.lookup(monitors, worker) do
      [{^worker, ref}] ->
        true = Process.demonitor(ref)
        true = :ets.delete(monitors, worker)
        {:noreply, %State{state | workers: [worker | workers]}}
      _ -> 
        {:noreply, state}
    end
  end

  ## Helpers

  ## Validate and parse configuration
  defp init([{:mfa, mfa} | rest], state), do: init(rest, %State{state | mfa: mfa})
  defp init([{:size, size} | rest], state), do: init(rest, %State{state | size: size})
  defp init([_ | rest], state), do: init(rest, state)
  defp init([], state) do
    send(self, :start_worker_supervisor)
    {:ok, state}
  end

  defp supervisor_spec(mfa) do
    import Supervisor.Spec

    opts = [restart: :temporary]
    supervisor(Pooly.WorkerSupervisor, [mfa], opts)
  end

  defp prepopulate(size, sup), do: prepopulate(size, sup, [])
  defp prepopulate(size, _sup, workers) when size < 1, do: workers
  defp prepopulate(size, sup, workers), do: prepopulate(size - 1, sup, [new_worker(sup) | workers])

  defp new_worker(sup) do
    {:ok, worker_pid} = Supervisor.start_child(sup, [[]])
    worker_pid
  end
end
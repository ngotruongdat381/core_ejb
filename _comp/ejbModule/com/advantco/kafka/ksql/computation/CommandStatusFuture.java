package com.advantco.kafka.ksql.computation;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class CommandStatusFuture implements Future<CommandStatus> {

	private final CommandId commandId;
	private final AtomicReference<CommandStatus> result;
	private final CommandStore commandStore;

	public CommandStatusFuture(CommandId commandId, CommandStore commandStore) {
		this.commandId = commandId;
		this.result = new AtomicReference<>(null);
		this.commandStore = commandStore; 
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCancelled() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isDone() {
		return result.get() != null;
	}

	@Override
	public CommandStatus get() throws InterruptedException, ExecutionException {
		synchronized (result) {
			while (result.get() == null) {
				result.wait();
			}
			commandStore.removeFromFutures(commandId);
			return result.get();
		}
	}

	@Override
	public CommandStatus get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
		long endTimeMs = System.currentTimeMillis() + unit.toMillis(timeout);
		synchronized (result) {
			while (System.currentTimeMillis() < endTimeMs && result.get() == null) {
				result.wait(Math.max(1, endTimeMs - System.currentTimeMillis()));
			}
			if (result.get() == null) {
				throw new TimeoutException();
			}
			commandStore.removeFromFutures(commandId);
			return result.get();
		}
	}

	public void complete(CommandStatus result) {
		synchronized (this.result) {
			this.result.set(result);
			this.result.notifyAll();
		}
	}
}

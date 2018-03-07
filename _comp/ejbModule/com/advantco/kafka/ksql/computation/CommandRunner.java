package com.advantco.kafka.ksql.computation;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import com.advantco.base.logging.ILogger;
import com.advantco.kafka.ksql.KsqlResource;
import com.advantco.kafka.utils.KLoggerWrapper;

import io.confluent.ksql.util.Pair;

public class CommandRunner implements Runnable, Closeable {
	private static final ILogger LOGGER = KLoggerWrapper.getLogger(CommandRunner.class);
	private final KsqlResource ksqlResource;
	private final CommandStore commandStore;
	private final AtomicBoolean closed;

	public CommandRunner(KsqlResource ksqlResource, CommandStore commandStore) {
		this.ksqlResource = ksqlResource;
		this.commandStore = commandStore;
		this.closed = new AtomicBoolean(false);;
	}

	@Override
	public void run() {
		try {
			while ( !closed.get() ) {
				ConsumerRecords<CommandId, Command> records = commandStore.getNewCommands();
				for ( ConsumerRecord<CommandId, Command> record : records ) {
					CommandId commandId = record.key();
					Command command = record.value();
					if ( command.getStatement() != null ) {
						executeStatement(command, commandId);
					} else {
						if ( LOGGER.isDebugEnabled() ) {
							LOGGER.debug("Skipping null statement for ID: " + commandId);
						}
					}
				}

			}
		} catch ( WakeupException wue ) {
			if ( LOGGER.isWarnEnabled() ) {
				LOGGER.warn("Exit CommandRunner from wakeup");
			}
			if ( !closed.get() ) {
				throw wue;
			}
		}
	}

	@Override
	public void close() throws IOException {
		closed.set(true);
		commandStore.close();
	}
	
	//Old
//	public void processPriorCommands() throws Exception {
//		final RestoreCommands restoreCommands = commandStore.getPriorCommands();
//		ksqlResource.handleStatements(priorCommands);
//	}
	
	public void processPriorCommands() throws Exception {
	    final RestoreCommands restoreCommands = commandStore.getRestoreCommands();
	    ksqlResource.handleRestoration(restoreCommands);
	}
	
	private void executeStatement(Command command, CommandId commandId) {
		try {
			ksqlResource.handleStatement(command, commandId);
		} catch ( WakeupException wue ) {
			throw wue;
		} catch ( Exception exception ) {
			if ( LOGGER.isWarnEnabled() ) {
				LOGGER.warn("Exception encountered during poll-parse-execute loop");
			}
		}
	}

}

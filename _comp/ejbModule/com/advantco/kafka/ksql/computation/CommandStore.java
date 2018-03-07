package com.advantco.kafka.ksql.computation;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.advantco.base.logging.ILogger;
import com.advantco.kafka.utils.KLoggerWrapper;

import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.Pair;

public class CommandStore implements Closeable {
	private static final long POLLING_TIMEOUT_FOR_COMMAND_TOPIC = 5000;
	private static final ILogger LOGGER = KLoggerWrapper.getLogger(CommandStore.class);
	private final String commandTopic;
	private final Consumer<CommandId, Command> commandConsumer;
	private final Producer<CommandId, Command> commandProducer;
	private final CommandIdAssigner commandIdAssigner;
	private final AtomicBoolean closed;
	private final Map<CommandId, CommandStatus> statusStore;
	private final Map<CommandId, CommandStatusFuture> statusFuture;

	public CommandStore(
			String commandTopic,
			Consumer<CommandId, Command> commandConsumer,
			Producer<CommandId, Command> commandProducer,
			CommandIdAssigner commandIdAssigner
			) {
		this.commandTopic = commandTopic;
		this.commandConsumer = commandConsumer;
		this.commandProducer = commandProducer;
		this.commandIdAssigner = commandIdAssigner;
		commandConsumer.assign(Collections.singleton(new TopicPartition(commandTopic, 0)));
		closed = new AtomicBoolean(false);
		
		this.statusStore = new HashMap<>();
		this.statusFuture = new HashMap<>();
	}

	@Override
	public void close() throws IOException {
		closed.set(true);
		commandConsumer.wakeup();
		commandProducer.close();
		commandConsumer.close();
	}

	public CommandId distributeStatement(
			String statementString,
			Statement statement,
			Map<String, Object> streamsProperties
			) throws Exception {

		CommandId commandId = commandIdAssigner.getCommandId(statement);
		Command command = new Command(statementString, streamsProperties);
		commandProducer.send(new ProducerRecord<>(commandTopic, commandId, command)).get();
		return commandId;
	}
	
	//old function
	public ConsumerRecords<CommandId, Command> getNewCommands() {
		return commandConsumer.poll(Long.MAX_VALUE);
	}
	
	//new function
    RestoreCommands getRestoreCommands() {
        final RestoreCommands restoreCommands = new RestoreCommands();

        Collection<TopicPartition> commandTopicPartitions = getTopicPartitionsForTopic(commandTopic);

        commandConsumer.seekToBeginning(commandTopicPartitions);

        LOGGER.debug("Reading prior command records");

        final Map<CommandId, ConsumerRecord<CommandId, Command>> commands = new LinkedHashMap<>();
        ConsumerRecords<CommandId, Command> records = commandConsumer.poll(POLLING_TIMEOUT_FOR_COMMAND_TOPIC);
        while (!records.isEmpty()) {
        	LOGGER.debug("Received {} records from poll", records.count());
            for (ConsumerRecord<CommandId, Command> record : records) {
                restoreCommands.addCommand(record.key(), record.value());
            }
            records = commandConsumer.poll(POLLING_TIMEOUT_FOR_COMMAND_TOPIC);
        }
        LOGGER.debug("Retrieved records:" + commands.size());
        return restoreCommands;
    }
    
	public List<Pair<CommandId, Command>> getPriorCommands() {
		List<Pair<CommandId, Command>> result = new ArrayList<>();
		for (ConsumerRecord<CommandId, Command> commandRecord : getAllPriorCommandRecords()) {
			CommandId commandId = commandRecord.key();
			Command command = commandRecord.value();
			if (command != null) {
				result.add(new Pair<>(commandId, command));
			}
		}

		return result;
	}

	public Future<CommandStatus> registerQueuedCommand(CommandId commandId) {
		statusStore.put(
		        commandId,
		        new CommandStatus(CommandStatus.Status.QUEUED, "Statement written to command topic")
		    );
		CommandStatusFuture result;
		synchronized (statusFuture) {
			result = statusFuture.get(commandId);
			if (result != null) {
				return result;
			} else {
				result = new CommandStatusFuture(commandId, this);
				statusFuture.put(commandId, result);
				return result;
			}
		}
	}

	public void completeCommand(CommandId commandId, CommandStatus commandStatus) {
		synchronized (statusFuture) {
			CommandStatusFuture cmdStatusFuture = statusFuture.get(commandId);
			if (cmdStatusFuture != null) {
				cmdStatusFuture.complete(commandStatus);
			} else {
				CommandStatusFuture newStatusFuture = new CommandStatusFuture(commandId, this);
				newStatusFuture.complete(commandStatus);
				statusFuture.put(commandId, newStatusFuture);
			}
		}
	}
	 
	public void storeCommandStatus(CommandId commandId, CommandStatus.Status status , String message) {
		statusStore.put(commandId, new CommandStatus(status, message));
	}
	
	public CommandStatus getStatus(CommandId commandId) {
	    return statusStore.get(commandId);
	  }

	private List<ConsumerRecord<CommandId, Command>> getAllPriorCommandRecords() {
		Collection<TopicPartition> commandTopicPartitions = getTopicPartitionsForTopic(commandTopic);
		commandConsumer.poll(0);
		commandConsumer.seekToBeginning(commandTopicPartitions);
		
		Map<TopicPartition, Long> currentOffsets = new HashMap<>();

		List<ConsumerRecord<CommandId, Command>> result = new ArrayList<>();
		Map<TopicPartition, Long> endOffsets = commandConsumer.endOffsets(commandTopicPartitions);
		do {
			while (!offsetsCaughtUp(currentOffsets, endOffsets)) {
				ConsumerRecords<CommandId, Command> records = commandConsumer.poll(30000);
				if (records.isEmpty()) {
					if ( LOGGER.isDebugEnabled() ) {
						LOGGER.debug("No records received after 30 seconds of polling; something may be wrong");
					}
				} else {
					for (ConsumerRecord<CommandId, Command> record : records) {
						result.add(record);
						TopicPartition recordTopicPartition =
								new TopicPartition(record.topic(), record.partition());
						Long currentOffset = currentOffsets.get(recordTopicPartition);
						if (currentOffset == null || currentOffset < record.offset()) {
							currentOffsets.put(recordTopicPartition, record.offset());
						}
					}
				}
			}
			endOffsets = commandConsumer.endOffsets(commandTopicPartitions);
		} while (!offsetsCaughtUp(currentOffsets, endOffsets));
		return result;
	}

	private Collection<TopicPartition> getTopicPartitionsForTopic(String topic) {
		List<PartitionInfo> partitionInfoList = commandConsumer.partitionsFor(topic);

		Collection<TopicPartition> result = new HashSet<>();
		for (PartitionInfo partitionInfo : partitionInfoList) {
			result.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
		}

		return result;
	}

	private boolean offsetsCaughtUp(Map<TopicPartition, Long> offsets, Map<TopicPartition, Long> endOffsets) {
		for (Map.Entry<TopicPartition, Long> endOffset : endOffsets.entrySet()) {
			long offset = offsets.getOrDefault(endOffset.getKey(), 0L);
			if (offset + 1 < endOffset.getValue()) {
				if ( LOGGER.isDebugEnabled() ) {
					LOGGER.debug("Consumed command records are not yet caught up with offset for partition " + endOffset.getKey().partition());
				}
				return false;
			}
		}
		return true;
	}

	public void removeFromFutures(CommandId commandId) {
		statusFuture.remove(commandId);
	}

	public CommandStatusFuture getCommandStatusFuture(CommandId commandId) {
		return statusFuture.get(commandId);
	}

	public CommandStatusFuture addStatusFuture(CommandId commandId, CommandStatusFuture cmdSttFuture) {
		return statusFuture.put(commandId, cmdSttFuture);
	}
}

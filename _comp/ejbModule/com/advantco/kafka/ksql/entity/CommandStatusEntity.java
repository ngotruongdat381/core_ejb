package com.advantco.kafka.ksql.entity;

import com.advantco.kafka.ksql.computation.CommandId;
import com.advantco.kafka.ksql.computation.CommandStatus;

public class CommandStatusEntity extends KsqlEntity{
	private final CommandId commandId;
	private final CommandStatus commandStatus;

	public CommandStatusEntity(String statementText, CommandId commandId, CommandStatus commandStatus) {
		super(statementText);
		this.commandId = commandId;
		this.commandStatus = commandStatus;
	}

	public CommandId getCommandId() {
		return commandId;
	}

	public CommandStatus getCommandStatus() {
		return commandStatus;
	}
}

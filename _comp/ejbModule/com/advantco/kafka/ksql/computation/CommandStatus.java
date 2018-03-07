package com.advantco.kafka.ksql.computation;

public class CommandStatus {
	public enum Status { QUEUED, PARSING, EXECUTING, SUCCESS, ERROR }

	private final Status status;
	private final String message;

	public CommandStatus(Status status, String message) {
		this.status = status;
		this.message = message;
	}

	public Status getStatus() {
		return status;
	}

	public String getMessage() {
		return message;
	}
}

package com.advantco.kafka.ksql.computation;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class CommandId {
	private final Type type;
	private final String entity;
	private final Action action;
	
	public enum Type {
		TOPIC,
		STREAM,
		TABLE,
		TERMINATE
	}
	
	public enum Action {
		CREATE,
		DROP,
		EXECUTE
	}
	public CommandId(final Type type, final String entity, final Action action) {
		this.type = type;
		this.entity = entity;
		this.action = action;
	}

	public CommandId(final String type,
					final String entity,
					final String action) {
		this(Type.valueOf(type.toUpperCase()), entity, Action.valueOf(action.toUpperCase()));
	}

	@JsonCreator
	public static CommandId fromString(String fromString) {
		String[] splitOnSlash = fromString.split("/", 3);
		if (splitOnSlash.length != 3) {
			throw new IllegalArgumentException("Expected a string of the form <type>/<entity>/<action>");
		}
		return new CommandId(splitOnSlash[0], splitOnSlash[1], splitOnSlash[2]);
	}

	public Type getType() {
		return type;
	}

	public String getEntity() {
		return entity;
	}

	public Action getAction() {
		return action;
	}
	
	@Override
	@JsonValue
	public String toString() {
		return String.format("%s/%s", type.toString().toLowerCase(), entity);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof CommandId)) {
			return false;
		}
		CommandId commandId = (CommandId) o;
		return getType() == commandId.getType()
				&& Objects.equals(getEntity(), commandId.getEntity());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getType(), getEntity());
	}
}

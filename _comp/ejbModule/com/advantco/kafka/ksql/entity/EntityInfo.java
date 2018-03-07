package com.advantco.kafka.ksql.entity;

import java.util.Objects;

import io.confluent.ksql.metastore.StructuredDataSource;

public class EntityInfo {
	private final String name;
	private final String topic;
	private final String format;
	public EntityInfo(
			String name,
			String topic,
			String format
			) {
		this.name = name;
		this.topic = topic;
		this.format = format;
	}

	public EntityInfo(StructuredDataSource sDataSource) {
		this(
			sDataSource.getName(),
			sDataSource.getKsqlTopic().getKafkaTopicName(),
			sDataSource.getKsqlTopic().getKsqlTopicSerDe().getSerDe().name()
		);
	}

	public String getName() {
		return name;
	}

	public String getTopic() {
		return topic;
	}

	public String getFormat() {
		return format;
	}
	
	@Override
	public int hashCode() {
		return  Objects.hash(getName(), getTopic(), getFormat());
	}
}

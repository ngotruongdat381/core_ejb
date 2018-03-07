package com.advantco.kafka.ksql.entity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import io.confluent.ksql.metastore.KsqlStream;

public class StreamList extends KsqlEntity {
	private Collection<EntityInfo> streams;

	public StreamList(String statementText, Collection<EntityInfo> streams) {
		super(statementText);
		this.streams = streams;
	}

	public static StreamList fromKsqlStreams(String statementText, Collection<KsqlStream> ksqlStreams) {
		Collection<EntityInfo> streamInfos = ksqlStreams.stream().map(EntityInfo::new).collect(Collectors.toList());
		return new StreamList(statementText, streamInfos);
	}

	public List<EntityInfo> getStreams() {
		return new ArrayList<>(streams);
	}
}

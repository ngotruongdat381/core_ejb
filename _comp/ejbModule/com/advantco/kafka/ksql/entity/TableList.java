package com.advantco.kafka.ksql.entity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import io.confluent.ksql.metastore.KsqlTable;

public class TableList extends KsqlEntity {
	private Collection<EntityInfo> tables;
	public TableList(String statementText, Collection<EntityInfo> tables) {
		super(statementText);
		this.tables = tables;
	}

	public static TableList fromKsqlTables(String statementText, Collection<KsqlTable> ksqlTables) {
		Collection<EntityInfo> tableInfos = ksqlTables.stream().map(EntityInfo::new).collect(Collectors.toList());
		return new TableList(statementText, tableInfos);
	}

	public List<EntityInfo> getTables() {
		return new ArrayList<>(tables);
	}
}

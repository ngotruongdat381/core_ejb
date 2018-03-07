package com.advantco.kafka.ksql.entity;

public class KsqlEntity {
	private final String statementText;

	public KsqlEntity(String statementText) {
		this.statementText = statementText;
	}

	public String getStatementText() {
		return statementText;
	}
}

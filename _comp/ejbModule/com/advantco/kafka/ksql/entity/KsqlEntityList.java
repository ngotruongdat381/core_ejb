package com.advantco.kafka.ksql.entity;

import java.util.ArrayList;
import java.util.Collection;

public class KsqlEntityList extends ArrayList<KsqlEntity> {
	public KsqlEntityList() {
	}

	public KsqlEntityList(int initialCapacity) {
		super(initialCapacity);
	}

	public KsqlEntityList(Collection<? extends KsqlEntity> c) {
		super(c);
	}
}

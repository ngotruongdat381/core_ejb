package com.advantco.kafka.record;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import com.advantco.kafka.utils.GeneralHelper;

public class KConsumerRecord<K, V> {
	private String topic;
	private int partition;
	private long offset;
	private long timestamp;
	private TimestampType timestampType;
	private Headers headers;
	private K key;
	private V value;

	public KConsumerRecord(ConsumerRecord<K, V> record) {
		this.key = record.key();
		this.value = record.value();
		this.topic = record.topic();
		this.partition = record.partition();
		this.offset = record.offset();
		this.timestamp = record.timestamp();
		this.headers = record.headers();
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getTopic() {
		return topic;
	}

	public int getPartition() {
		return partition;
	}

	public long getOffset() {
		return offset;
	}

	public TimestampType getTimestampType() {
		return timestampType;
	}

	public Headers getHeaders() {
		return headers;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("KafkaConsumerRecord [").append(GeneralHelper.NEW_LINE).append("TimeStamp = ").append(String.valueOf(timestamp)).append(GeneralHelper.NEW_LINE).append("Key = ").append(String.valueOf(key)).append(GeneralHelper.NEW_LINE).append("Value = ").append(String.valueOf(value)).append(GeneralHelper.NEW_LINE).append("Topic = ").append(topic)
				.append(GeneralHelper.NEW_LINE).append("Partition = ").append(String.valueOf(partition)).append("]");
		return sb.toString();
	}

}

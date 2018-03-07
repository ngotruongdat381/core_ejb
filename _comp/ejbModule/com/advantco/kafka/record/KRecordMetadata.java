package com.advantco.kafka.record;

import org.apache.kafka.clients.producer.RecordMetadata;

import com.advantco.kafka.utils.GeneralHelper;

public class KRecordMetadata {
	private RecordMetadata recordMetadata;

	public KRecordMetadata(RecordMetadata recordMetadata) {
		this.recordMetadata = recordMetadata;
	}

	public long getOffset() {
		return recordMetadata.offset();
	}

	public int getPartition() {
		return recordMetadata.partition();
	}

	public int getSerializedKeySize() {
		return recordMetadata.serializedKeySize();
	}

	public int getSerializedValueSize() {
		return recordMetadata.serializedValueSize();
	}

	public long getTimestamp() {
		return recordMetadata.timestamp();
	}

	public String getTopic() {
		return recordMetadata.topic();
	}

	public String toXML() {
		StringBuilder sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>").append("<Record>").append("<Offset>").append(recordMetadata.offset()).append("</Offset>").append("<Partition>").append(recordMetadata.partition()).append("</Partition>").append("<SerializedKeySize>").append(recordMetadata.serializedKeySize()).append("</SerializedKeySize>").append("<SerializedValueSize>")
				.append(recordMetadata.serializedValueSize()).append("</SerializedValueSize>").append("<Timestamp>").append(recordMetadata.timestamp()).append("</Timestamp>").append("<Topic>").append(recordMetadata.topic()).append("</Topic>").append("</Record>");
		return sb.toString();
	}

	public String toJSON() {
		StringBuilder sb = new StringBuilder("{").append("\"Offset\":").append(recordMetadata.offset()).append(",").append("\"Partition\":").append(recordMetadata.partition()).append(",").append("\"SerializedKeySize\":").append(recordMetadata.serializedKeySize()).append(",").append("\"SerializedValueSize\":").append(recordMetadata.serializedValueSize()).append(",").append("\"Timestamp\":")
				.append(recordMetadata.timestamp()).append(",").append("\"Topic\":\"").append(recordMetadata.topic()).append("\"").append("}");
		return sb.toString();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("KafkaRecordMetadata [").append(GeneralHelper.NEW_LINE).append("Offset = ").append(recordMetadata.offset()).append(GeneralHelper.NEW_LINE).append("Partition = ").append(recordMetadata.partition()).append(GeneralHelper.NEW_LINE).append("SerializedKeySize = ").append(recordMetadata.serializedKeySize()).append(GeneralHelper.NEW_LINE)
				.append("SerializedValueSize = ").append(recordMetadata.serializedValueSize()).append(GeneralHelper.NEW_LINE).append("Timestamp = ").append(recordMetadata.timestamp()).append(GeneralHelper.NEW_LINE).append("Topic = ").append(recordMetadata.topic()).append("]");
		return sb.toString();
	}
}

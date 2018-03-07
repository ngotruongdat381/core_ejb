package com.advantco.kafka.builder;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;

import com.advantco.base.variablesubstitution.VariableDefinitions;
import com.advantco.base.variablesubstitution.VariableSubstitution;
import com.advantco.base.variablesubstitution.VariableValues;
import com.advantco.kafka.exception.InvalidDataFormatException;
import com.advantco.kafka.exception.KBaseException;
import com.advantco.kafka.format.AdvDataFormat;
import com.advantco.kafka.format.avro.AdvArvoDataFormat;
import com.advantco.kafka.format.avro.KAvroDeserializer;
import com.advantco.kafka.format.avro.KAvroSerializer;
import com.sap.engine.interfaces.messaging.api.Message;

public class DataFormatBuilder implements IConfigBuilder {
	public static final String ADV_AVRO_SCHEMA_VALUE = "adv.avro.schema.value";
	AdvDataFormat dataformat;
	boolean isSender;
	protected Class<?> recordKeyType;
	protected Class<?> recordValueType;

	DataFormatBuilder() {
		this.dataformat = null;
		this.recordKeyType = String.class;
		this.recordValueType = byte[].class;
		isSender = true;
	}

	DataFormatBuilder(AdvDataFormat dataformat, Class<?> recordKeyType, Class<?> recordValueType, boolean sender) {
		this.dataformat = dataformat;
		this.recordKeyType = recordKeyType;
		this.recordValueType = recordValueType;
		isSender = sender;
	}

	@Override
	public Properties buildConfigProperties() throws KBaseException {
		return buildConfigProperties(new Properties());
	}

	@Override
	public Properties buildConfigProperties(Properties properties) throws KBaseException {
		if (isSender) {
			buildConfigConsumer(properties);
		} else {
			buildConfigProducer(properties);
		}
		return properties;
	}

	private void buildConfigConsumer(Properties properties) throws InvalidDataFormatException {
		if (dataformat != null && dataformat instanceof AdvArvoDataFormat) {
			properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.serdeFrom(recordKeyType).deserializer().getClass().getName());
			properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAvroDeserializer.class);
			properties.put(ADV_AVRO_SCHEMA_VALUE, dataformat.getValue());
		} else {
			properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.serdeFrom(recordKeyType).deserializer().getClass().getName());
			properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.serdeFrom(recordValueType).deserializer().getClass().getName());
		}
	}

	private void buildConfigProducer(Properties properties) throws KBaseException {
		if (dataformat != null && dataformat instanceof AdvArvoDataFormat) {
			properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.serdeFrom(recordKeyType).serializer().getClass().getName());
			properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAvroSerializer.class);
			properties.put(ADV_AVRO_SCHEMA_VALUE, dataformat.getValue());
		} else {
			properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.serdeFrom(recordKeyType).serializer().getClass().getName());
			properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.serdeFrom(recordValueType).serializer().getClass().getName());
		}
	}

	public AdvDataFormat getDataformat() {
		return dataformat;
	}

	public void setDataformat(AdvDataFormat dataformat) {
		this.dataformat = dataformat;
	}

	public boolean isSender() {
		return isSender;
	}

	public void setSender(boolean isSender) {
		this.isSender = isSender;
	}

	public Class<?> getRecordKeyType() {
		return recordKeyType;
	}

	public void setRecordKeyType(Class<?> recordKeyType) {
		this.recordKeyType = recordKeyType;
	}

	public Class<?> getRecordValueType() {
		return recordValueType;
	}

	public void setRecordValueType(Class<?> recordValueType) {
		this.recordValueType = recordValueType;
	}

	@Override
	public void cleanUpResource() {
	}

	@Override
	public Object applyVariableSubstitution(Message messageRequest, Message messageResponse, VariableDefinitions variableDefinitions, VariableValues varValues) throws Exception {
		DataFormatBuilder newDataFormat = new DataFormatBuilder();
		AdvDataFormat newAdvDataformat = null;
		if (dataformat != null && dataformat instanceof AdvArvoDataFormat) {
			newAdvDataformat = new AdvArvoDataFormat();
			((AdvArvoDataFormat) newAdvDataformat).setAvroSchemaType(((AdvArvoDataFormat)dataformat).getAvroSchemaType());
			((AdvArvoDataFormat) newAdvDataformat).setSchemaFromFile(VariableSubstitution.processVariableSubstitution(((AdvArvoDataFormat) dataformat).getSchemaFromFile(), messageRequest, messageResponse, variableDefinitions, varValues));
			((AdvArvoDataFormat) newAdvDataformat).setSchemaInJson(VariableSubstitution.processVariableSubstitution(((AdvArvoDataFormat) dataformat).getSchemaInJson(), messageRequest, messageResponse, variableDefinitions, varValues));
		}
		newDataFormat.setDataformat(newAdvDataformat);
		newDataFormat.setRecordKeyType(recordKeyType);
		newDataFormat.setRecordValueType(recordValueType);
		newDataFormat.setSender(isSender);
		return newDataFormat;
	}

}

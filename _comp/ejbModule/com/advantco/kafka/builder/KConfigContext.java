package com.advantco.kafka.builder;

import java.util.Properties;

import com.advantco.base.SSLAuth;
import com.advantco.base.StringUtil;
import com.advantco.base.variablesubstitution.VariableDefinitions;
import com.advantco.base.variablesubstitution.VariableValues;
import com.advantco.kafka.exception.KBaseException;
import com.advantco.kafka.format.AdvDataFormat;
import com.sap.engine.interfaces.messaging.api.Message;

public class KConfigContext {
	private SecurityConfigBuilder auth;
	private CommonConfigBuilder common;
	private DataFormatBuilder dataFormat;
	private ProducerConfigBuilder producer;
	private ConsumerConfigBuilder consumer;
	private StreamConfigBuilder stream;

	private KConfigContext() {
		auth = null;
		common = null;
		dataFormat = null;
		producer = null;
		consumer = null;
		stream = null;
	}

	public SecurityConfigBuilder createAuthConfig(String mechanism, String user, String pass, SSLAuth sslAuth) {
		auth = new SecurityConfigBuilder(mechanism, user, pass, sslAuth);
		return auth;
	}

	public CommonConfigBuilder createCommonConfig(String broker, String reqTime, String backOffMax, String backOff, String client) {
		common = new CommonConfigBuilder(broker, reqTime, backOffMax, backOff, client);
		return common;
	}

	public DataFormatBuilder createDataFormat(AdvDataFormat dataformat, Class<?> recordKeyType, Class<?> recordValueType, boolean sender) {
		dataFormat = new DataFormatBuilder(dataformat, recordKeyType, recordValueType, sender);
		return dataFormat;
	}

	public ProducerConfigBuilder createProducerConfig(String maxBlock, String retries, String acks, String compress) {
		producer = new ProducerConfigBuilder(maxBlock, retries, acks, compress);
		return producer;
	}

	public ConsumerConfigBuilder createConsumerConfig(String pollTimeout, String offset, String group) {
		consumer = new ConsumerConfigBuilder(pollTimeout, offset, group);
		return consumer;
	}

	public Properties buildConfigContext() throws KBaseException {
		return buildConfigContext(new Properties());
	}

	public Properties buildConfigContext(Properties properties) throws KBaseException {
		if (common != null) {
			common.buildConfigProperties(properties);
		}
		if (auth != null) {
			auth.buildConfigProperties(properties);
		}
		if (dataFormat != null) {
			dataFormat.buildConfigProperties(properties);
		}
		if (producer != null) {
			producer.buildConfigProperties(properties);
		}
		if (consumer != null) {
			consumer.buildConfigProperties(properties);
		}
		if (stream != null) {
			stream.buildConfigProperties(properties);
		}
		return properties;
	}

	public SecurityConfigBuilder getSecurityConfigBuilder() {
		if (auth == null) {
			auth = new SecurityConfigBuilder();
		}
		return auth;
	}

	public void setSecurityConfigBuilder(SecurityConfigBuilder auth) {
		this.auth = auth;
	}

	public CommonConfigBuilder getCommonConfigBuilder() {
		if (common == null) {
			common = new CommonConfigBuilder();
		}
		return common;
	}

	public void setCommonConfigBuilder(CommonConfigBuilder common) {
		this.common = common;
	}

	public ConsumerConfigBuilder getConsumerConfigBuilder() {
		if (consumer == null) {
			consumer = new ConsumerConfigBuilder();
		}
		return consumer;
	}

	public void setConsumerConfigBuilder(ConsumerConfigBuilder consumer) {
		this.consumer = consumer;
	}

	public ProducerConfigBuilder getProducerConfigBuilder() {
		if (producer == null) {
			producer = new ProducerConfigBuilder();
		}
		return producer;
	}

	public void setProducerConfigBuilder(ProducerConfigBuilder producer) {
		this.producer = producer;
	}

	public StreamConfigBuilder getStreamConfigBuilder() {
		if (stream == null) {
			stream = new StreamConfigBuilder();
		}
		return stream;
	}

	public void setStreamConfigBuilder(StreamConfigBuilder stream) {
		this.stream = stream;
	}

	public DataFormatBuilder getDataFormatBuilder() {
		if (dataFormat == null) {
			dataFormat = new DataFormatBuilder();
		}
		return dataFormat;
	}

	public void setDataFormatBuilder(DataFormatBuilder dataFormat) {
		this.dataFormat = dataFormat;
	}

	public KConfigContext applyVariableSubstitution(Message messageRequest, Message messageResponse, VariableDefinitions variableDefinitions, VariableValues varValues) throws Exception {
		KConfigContext newKConfig = KConfigContextFactory.getConfigContext();
		if (common != null) {
			newKConfig.setCommonConfigBuilder((CommonConfigBuilder) common.applyVariableSubstitution(messageRequest, messageResponse, variableDefinitions, varValues));
		}
		if (auth != null) {
			newKConfig.setSecurityConfigBuilder((SecurityConfigBuilder) auth.applyVariableSubstitution(messageRequest, messageResponse, variableDefinitions, varValues));
		}
		if (consumer != null) {
			newKConfig.setConsumerConfigBuilder((ConsumerConfigBuilder) consumer.applyVariableSubstitution(messageRequest, messageResponse, variableDefinitions, varValues));
		}
		if (producer != null) {
			newKConfig.setProducerConfigBuilder((ProducerConfigBuilder) producer.applyVariableSubstitution(messageRequest, messageResponse, variableDefinitions, varValues));
		}
		if (dataFormat != null) {
			newKConfig.setDataFormatBuilder((DataFormatBuilder) dataFormat.applyVariableSubstitution(messageRequest, messageResponse, variableDefinitions, varValues));
		}
		return newKConfig;
	}

	public void cleanupResource() {
		auth.cleanUpResource();
	}

	public static boolean isValidNumericValue(String value) {
		if (StringUtil.nullOrBlank(value)) {
			return false;
		}
		String newValue = new String(value);
		if (newValue.charAt(0) == '+' || newValue.charAt(0) == '-') {
			newValue = newValue.substring(1);
		}
		int length = newValue.length();
		for (int i = 0; i < length; i++) {
			if (!Character.isDigit(newValue.charAt(i))) {
				return false;
			}
		}
		return true;
	}

	public static class KConfigContextFactory {
		private KConfigContextFactory() {
		}

		public static KConfigContext getConfigContext() {
			return new KConfigContext();
		}
	}
}

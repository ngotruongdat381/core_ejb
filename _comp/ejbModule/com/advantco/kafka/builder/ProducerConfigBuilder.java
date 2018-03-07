package com.advantco.kafka.builder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.ProducerConfig;

import com.advantco.base.NameValuePair;
import com.advantco.base.StringUtil;
import com.advantco.base.variablesubstitution.VariableDefinitions;
import com.advantco.base.variablesubstitution.VariableSubstitution;
import com.advantco.base.variablesubstitution.VariableValues;
import com.advantco.kafka.exception.KBaseException;
import com.sap.engine.interfaces.messaging.api.Message;

public class ProducerConfigBuilder extends CommonConfigBuilder implements IConfigBuilder {
	public static final Set<String> KNOWN_PRODUCER_CONFIGS;
	private String maxBlock;
	private String retries;
	private String retryBackOff;
	private String acks;
	private String compressionType;
	private boolean enableAdvanced;
	private boolean enableDuplicateHandling;
	private boolean enableIdempotence;
	private String maxInFlightRequest;
	private String bufferMemory;

	// Other parameters are not in KAFKA Configuration;
	private String topic;
	private String partition;
	private Collection<NameValuePair> recordHeaders;

	static {
		Set<String> prodConfigs = new HashSet<String>();
		prodConfigs.add(ProducerConfig.MAX_BLOCK_MS_CONFIG);
		prodConfigs.add(ProducerConfig.RETRIES_CONFIG);
		prodConfigs.add(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
		prodConfigs.add(ProducerConfig.ACKS_CONFIG);
		prodConfigs.add(ProducerConfig.COMPRESSION_TYPE_CONFIG);
		prodConfigs.add(ProducerConfig.BUFFER_MEMORY_CONFIG);
		prodConfigs.add(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
		prodConfigs.add(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
		KNOWN_PRODUCER_CONFIGS = Collections.unmodifiableSet(prodConfigs);
	}

	ProducerConfigBuilder() {
		super();
		this.maxBlock = null;
		this.retries = null;
		this.retryBackOff = null;
		this.acks = null;
		this.compressionType = null;
		enableAdvanced = false;
		enableDuplicateHandling = false;
		this.enableIdempotence = false;
		this.maxInFlightRequest = null;
		this.bufferMemory = null;
	}

	ProducerConfigBuilder(String maxBlock, String retries, String acks, String compressionType) {
		super();
		this.maxBlock = maxBlock;
		this.retries = retries;
		this.retryBackOff = null;
		this.acks = acks;
		this.compressionType = compressionType;
		enableAdvanced = false;
		enableDuplicateHandling = false;
		this.enableIdempotence = false;
		this.maxInFlightRequest = null;
		this.bufferMemory = null;
	}

	ProducerConfigBuilder(String maxBlock, String retries, String retryBackOff, String acks, String compressionType, boolean enableAdvanced, boolean enableIdempotence, String maxInFlightRequest, String bufferMemory) {
		super();
		this.maxBlock = maxBlock;
		this.retries = retries;
		this.retryBackOff = retryBackOff;
		this.acks = acks;
		this.compressionType = compressionType;
		this.enableAdvanced = enableAdvanced;
		if (StringUtil.notNullNorBlank(maxInFlightRequest)) {
			enableDuplicateHandling = true;
		} else {
			enableDuplicateHandling = false;
		}
		this.enableIdempotence = enableIdempotence;
		this.maxInFlightRequest = maxInFlightRequest;
		this.bufferMemory = bufferMemory;
	}

	@Override
	public Properties buildConfigProperties() throws KBaseException {
		Properties newProperties = new Properties();
		super.buildConfigProperties(newProperties);
		return buildConfigProperties(new Properties());
	}

	@Override
	public Properties buildConfigProperties(Properties properties) throws KBaseException {
		super.buildConfigProperties(properties);
		checkAndSetDefaultValue();
		
		if (maxBlock != null) {
			properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlock);
		}
		if (retries != null) {
			properties.put(ProducerConfig.RETRIES_CONFIG, retries);
		}
		if (retryBackOff != null) {
			properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackOff);
		}
		if (acks != null) {
			properties.put(ProducerConfig.ACKS_CONFIG, acks);
		}
		if (compressionType != null) {
			properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
		}

		if (!enableAdvanced) {
			return properties;
		}

		if (enableDuplicateHandling) {
			properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
			if (maxInFlightRequest != null) {
				properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequest);
			}
		}

		if (enableIOBehavior) {
			if (bufferMemory != null) {
				properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
			}
		}

		// if (otherAdvancedConfig != null) {
		// Iterator<NameValuePair> it = otherAdvancedConfig.iterator();
		// while (it.hasNext()) {
		// NameValuePair config = (NameValuePair) it.next();
		// if (!KNOWN_PRODUCER_CONFIGS.contains(config.getName()) ||
		// !CommonConfigBuilder.KNOWN_COMMON_CONFIGS.contains(config.getName()))
		// {
		// properties.put(config.getName(), config.getValue());
		// }
		// }
		// }
		return properties;
	}

	private void checkAndSetDefaultValue() {
		if (!KConfigContext.isValidNumericValue(maxBlock)) {
			maxBlock = "60000";
		}
		if (!KConfigContext.isValidNumericValue(retries)) {
			retries = "0";
		}
		if (!KConfigContext.isValidNumericValue(retryBackOff)) {
			retryBackOff = "100";
		}
		if (!enableAdvanced) {
			return;
		}
		if (enableDuplicateHandling) {
			if (maxInFlightRequest != null && !KConfigContext.isValidNumericValue(maxInFlightRequest)) {
				maxInFlightRequest = "5";
			}
		}
		if (enableIOBehavior) {
			if (bufferMemory != null && !KConfigContext.isValidNumericValue(bufferMemory)) {
				bufferMemory = "33554432";
			}
		}
	}

	public String getMaxBlock() {
		return maxBlock;
	}

	public void setMaxBlock(String maxBlock) {
		this.maxBlock = maxBlock;
	}

	public String getRetries() {
		return retries;
	}

	public void setRetries(String retries) {
		this.retries = retries;
	}

	public String getRetryBackOff() {
		return retryBackOff;
	}

	public void setRetryBackOff(String retryBackOff) {
		this.retryBackOff = retryBackOff;
	}

	public String getAcks() {
		return acks;
	}

	public void setAcks(String acks) {
		this.acks = acks;
	}

	public String getCompressionType() {
		return compressionType;
	}

	public void setCompressionType(String compressionType) {
		this.compressionType = compressionType;
	}

	public boolean isEnableAdvanced() {
		return enableAdvanced;
	}

	public void setEnableAdvanced(boolean enableAdvanced) {
		this.enableAdvanced = enableAdvanced;
	}

	public boolean isEnableDuplicateHandling() {
		return enableDuplicateHandling;
	}

	public boolean isEnableIdempotence() {
		return enableIdempotence;
	}

	public void setEnableIdempotence(boolean enableIdempotence) {
		enableDuplicateHandling = true;
		this.enableIdempotence = enableIdempotence;
	}

	public String getMaxInFlightRequest() {
		return maxInFlightRequest;
	}

	public void setMaxInFlightRequest(String maxInFlightRequest) {
		enableDuplicateHandling = true;
		this.maxInFlightRequest = maxInFlightRequest;
	}

	public String getBufferMemory() {
		return bufferMemory;
	}

	public void setBufferMemory(String bufferMemory) {
		enableIOBehavior = true;
		this.bufferMemory = bufferMemory;
	}

	// public boolean isEnableTransactionParameter() {
	// return enableTransactionParameter;
	// }

	// public String getTransactionTimeout() {
	// return transactionTimeout;
	// }
	//
	// public void setTransactionTimeout(String transactionTimeout) {
	// this.transactionTimeout = transactionTimeout;
	// }
	//
	// public String getTransactionId() {
	// return transactionId;
	// }
	//
	// public void setTransactionId(String transactionId) {
	// this.transactionId = transactionId;
	// }

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getPartition() {
		return partition;
	}

	public void setPartition(String partition) {
		if (KConfigContext.isValidNumericValue(partition)) {
			this.partition = partition;
		} else {
			this.partition = null;
		}
	}

	public Collection<NameValuePair> getRecordHeaders() {
		return recordHeaders;
	}

	public void addRecordHeader(String key, String value) {
		if (StringUtil.notNullNorBlank(key)) {
			if (this.recordHeaders == null) {
				this.recordHeaders = new ArrayList<NameValuePair>();
			}
			this.recordHeaders.add(new NameValuePair(key, value));
		}
	}

	public void setRecordHeaders(Collection<NameValuePair> recordHeaders) {
		this.recordHeaders = recordHeaders;
	}

	@Override
	public void cleanUpResource() {
	}

	@Override
	public Object applyVariableSubstitution(Message messageRequest, Message messageResponse, VariableDefinitions variableDefinitions, VariableValues varValues) throws Exception {
		ProducerConfigBuilder newProducer = new ProducerConfigBuilder();
		super.applyVariableSubstitution(newProducer, messageRequest, messageResponse, variableDefinitions, varValues);
		newProducer.setMaxBlock(VariableSubstitution.processVariableSubstitution(maxBlock, messageRequest, messageResponse, variableDefinitions, varValues));
		newProducer.setRetries(VariableSubstitution.processVariableSubstitution(retries, messageRequest, messageResponse, variableDefinitions, varValues));
		newProducer.setRetryBackOff(VariableSubstitution.processVariableSubstitution(retryBackOff, messageRequest, messageResponse, variableDefinitions, varValues));
		newProducer.setAcks(acks);
		newProducer.setCompressionType(compressionType);
		newProducer.setEnableAdvanced(enableAdvanced);
		newProducer.setBufferMemory(VariableSubstitution.processVariableSubstitution(bufferMemory, messageRequest, messageResponse, variableDefinitions, varValues));
		newProducer.setEnableIdempotence(enableIdempotence);
		newProducer.setMaxInFlightRequest(VariableSubstitution.processVariableSubstitution(maxInFlightRequest, messageRequest, messageResponse, variableDefinitions, varValues));
		// if (otherAdvancedConfig != null) {
		// Iterator<NameValuePair> it = otherAdvancedConfig.iterator();
		// while (it.hasNext()) {
		// NameValuePair oldConfig = (NameValuePair) it.next();
		// newProducer.addotherAdvancedConfig(VariableSubstitution.processVariableSubstitution(oldConfig.getName(),
		// messageRequest, messageResponse, variableDefinitions, varValues),
		// VariableSubstitution.processVariableSubstitution(oldConfig.getValue(),
		// messageRequest, messageResponse, variableDefinitions, varValues));
		// }
		// }

		newProducer.setTopic(VariableSubstitution.processVariableSubstitution(topic, messageRequest, messageResponse, variableDefinitions, varValues));
		newProducer.setPartition(VariableSubstitution.processVariableSubstitution(partition, messageRequest, messageResponse, variableDefinitions, varValues));
		if (recordHeaders != null) {
			Iterator<NameValuePair> it = recordHeaders.iterator();
			while (it.hasNext()) {
				NameValuePair oldConfig = (NameValuePair) it.next();
				newProducer.addRecordHeader(VariableSubstitution.processVariableSubstitution(oldConfig.getName(), messageRequest, messageResponse, variableDefinitions, varValues),
						VariableSubstitution.processVariableSubstitution(oldConfig.getValue(), messageRequest, messageResponse, variableDefinitions, varValues));
			}
		}

		return newProducer;
	}

}

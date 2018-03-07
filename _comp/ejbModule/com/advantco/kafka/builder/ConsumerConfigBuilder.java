package com.advantco.kafka.builder;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.advantco.base.StringUtil;
import com.advantco.base.variablesubstitution.VariableDefinitions;
import com.advantco.base.variablesubstitution.VariableSubstitution;
import com.advantco.base.variablesubstitution.VariableValues;
import com.advantco.kafka.exception.KBaseException;
import com.advantco.kafka.utils.GeneralHelper;
import com.sap.engine.interfaces.messaging.api.Message;

public class ConsumerConfigBuilder extends CommonConfigBuilder implements IConfigBuilder {
	public static final Set<String> KNOWN_CONSUMER_CONFIGS;
	private String pollTimeOut;
	private String consumeOffset;
	private String groupId;
	private boolean enableAdvanced;
	private boolean enableMaxPoll;
	private String maxPollRecords;
	private String maxPollInterval;
	private boolean enableAutoCommit;
	private String autoCommitInterval;
	private boolean enableFetchParameter;
	private String fetchMinBytes;
	private String fetchMaxBytes;
	private String fetchMaxWait;
	private boolean enableSessionConfig;
	private String sessionTimeout;
	private String heartbeatInterval;

	// Other parameters are not in KAFKA Configuration;
	private String topics;

	static {
		Set<String> consConfigs = new HashSet<String>();
		consConfigs.add(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
		consConfigs.add(ConsumerConfig.GROUP_ID_CONFIG);
		consConfigs.add(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
		consConfigs.add(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
		consConfigs.add(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
		consConfigs.add(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
		consConfigs.add(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
		consConfigs.add(ConsumerConfig.FETCH_MAX_BYTES_CONFIG);
		consConfigs.add(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
		consConfigs.add(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
		consConfigs.add(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
		KNOWN_CONSUMER_CONFIGS = Collections.unmodifiableSet(consConfigs);
	}

	ConsumerConfigBuilder() {
		super();
		this.pollTimeOut = null;
		this.consumeOffset = null;
		this.groupId = null;
		this.enableAdvanced = false;
		this.enableMaxPoll = false;
		this.maxPollRecords = null;
		this.maxPollInterval = null;
		this.enableAutoCommit = true;
		this.autoCommitInterval = null;
		this.enableFetchParameter = false;
		this.fetchMinBytes = null;
		this.fetchMaxBytes = null;
		this.fetchMaxWait = null;
		this.enableSessionConfig = false;
		this.sessionTimeout = null;
		this.heartbeatInterval = null;
	}

	ConsumerConfigBuilder(String pollTimeOut, String consumeOffset, String groupId) {
		super();
		this.pollTimeOut = pollTimeOut;
		this.consumeOffset = consumeOffset;
		this.groupId = groupId;
		enableAdvanced = false;
		this.maxPollRecords = null;
		this.maxPollInterval = null;
		enableAutoCommit = true;
		this.autoCommitInterval = null;
		this.fetchMinBytes = null;
		this.fetchMaxBytes = null;
		this.fetchMaxWait = null;
		this.sessionTimeout = null;
		this.heartbeatInterval = null;
	}

	ConsumerConfigBuilder(String pollTimeOut, String consumeOffset, String groupId, boolean enableAdvanced, String maxPollRecords, String maxPollInterval, boolean enableAutoCommit, String autoCommitInterval, String fetchMinBytes, String fetchMaxBytes, String fetchMaxWait, String sessionTimeout,
			String heartbeatInterval) {
		super();
		this.pollTimeOut = pollTimeOut;
		this.consumeOffset = consumeOffset;
		this.groupId = groupId;
		this.enableAdvanced = enableAdvanced;
		if (StringUtil.notNullNorBlank(maxPollInterval)) {
			enableMaxPoll = true;
		} else {
			enableMaxPoll = false;
		}
		this.maxPollRecords = maxPollRecords;
		this.maxPollInterval = maxPollInterval;
		this.enableAutoCommit = enableAutoCommit;
		this.autoCommitInterval = autoCommitInterval;
		if (StringUtil.notNullNorBlank(fetchMinBytes) || StringUtil.notNullNorBlank(fetchMaxBytes)) {
			enableFetchParameter = true;
		} else {
			enableFetchParameter = false;
		}
		this.fetchMinBytes = fetchMinBytes;
		this.fetchMaxBytes = fetchMaxBytes;
		this.fetchMaxWait = fetchMaxWait;
		if (StringUtil.notNullNorBlank(sessionTimeout) || StringUtil.notNullNorBlank(heartbeatInterval)) {
			enableSessionConfig = true;
		} else {
			enableSessionConfig = false;
		}
		this.sessionTimeout = sessionTimeout;
		this.heartbeatInterval = heartbeatInterval;
	}

	@Override
	public Properties buildConfigProperties() throws KBaseException {
		Properties newProperties = new Properties();
		super.buildConfigProperties(newProperties);
		return buildConfigProperties(newProperties);
	}

	@Override
	public Properties buildConfigProperties(Properties properties) throws KBaseException {
		super.buildConfigProperties(properties);
		checkAndSetDefaultValue();
		
		if (consumeOffset != null) {
			properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumeOffset);
		}
		if (groupId != null) {
			properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		}
		if (maxPollRecords != null) {
			properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
		}
		if (!enableAdvanced) {
			return properties;
		}

		if (enableMaxPoll) {
			if (maxPollInterval != null) {
				properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
			}
		}

		if (enableAutoCommit) {
			properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
			if (autoCommitInterval != null) {
				properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
			}
		} else {
			properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		}

		if (enableFetchParameter) {
			if (fetchMinBytes != null) {
				properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
			}
			if (fetchMaxBytes != null) {
				properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
			}
		}

		if (enableSessionConfig) {
			if (sessionTimeout != null) {
				properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
			}
			if (heartbeatInterval != null) {
				properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval);
			}
		}

		if (enableIOBehavior) {
			if (fetchMaxWait != null) {
				properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWait);
			}
		}

		// if (null != otherAdvantcedConfig) {
		// Iterator<NameValuePair> it = otherAdvantcedConfig.iterator();
		// while (it.hasNext()) {
		// NameValuePair config = (NameValuePair) it.next();
		// if (!KNOWN_CONSUMER_CONFIGS.contains(config.getName()) ||
		// !CommonConfigBuilder.KNOWN_COMMON_CONFIGS.contains(config.getName()))
		// {
		// properties.put(config.getName(), config.getValue());
		// }
		// }
		// }
		return properties;
	}

	private void checkAndSetDefaultValue() {

		if (StringUtil.nullOrBlank(groupId)) {
			groupId = GeneralHelper.getGroupId();
		}

		if (maxPollRecords != null && !KConfigContext.isValidNumericValue(maxPollRecords)) {
			maxPollRecords = "500";
		}

		if (!enableAdvanced) {
			return;
		}

		if (enableMaxPoll) {
			if (maxPollInterval != null && !KConfigContext.isValidNumericValue(maxPollInterval)) {
				maxPollInterval = "300000";
			}
		}
		if (enableAutoCommit) {
			if (autoCommitInterval != null && !KConfigContext.isValidNumericValue(autoCommitInterval)) {
				autoCommitInterval = "5000";
			}
		}
		if (enableFetchParameter) {
			if (fetchMinBytes != null && !KConfigContext.isValidNumericValue(fetchMinBytes)) {
				fetchMinBytes = "1";
			}
			if (fetchMaxBytes != null && !KConfigContext.isValidNumericValue(fetchMaxBytes)) {
				fetchMaxBytes = "52428800";
			}
		}
		if (enableSessionConfig) {
			if (sessionTimeout != null && !KConfigContext.isValidNumericValue(sessionTimeout)) {
				sessionTimeout = "10000";
			}
			if (heartbeatInterval != null && !KConfigContext.isValidNumericValue(heartbeatInterval)) {
				heartbeatInterval = "3000";
			}
		}
		if (enableIOBehavior) {
			if (fetchMaxWait != null && !KConfigContext.isValidNumericValue(fetchMaxWait)) {
				fetchMaxWait = "500";
			}
		}
	}

	public String getPollTimeOut() {
		return pollTimeOut;
	}

	public void setPollTimeOut(String pollTimeOut) {
		this.pollTimeOut = pollTimeOut;
	}

	public String getConsumeOffset() {
		return consumeOffset;
	}

	public void setConsumeOffset(String consumeOffset) {
		this.consumeOffset = consumeOffset;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public boolean isEnableAdvanced() {
		return enableAdvanced;
	}

	public void setEnableAdvanced(boolean enableAdvanced) {
		this.enableAdvanced = enableAdvanced;
	}

	public boolean isEnableMaxPoll() {
		return enableMaxPoll;
	}

	public String getMaxPollRecords() {
		return maxPollRecords;
	}

	public void setMaxPollRecords(String maxPollRecords) {
		this.maxPollRecords = maxPollRecords;
	}

	public String getMaxPollInterval() {
		return maxPollInterval;
	}

	public void setMaxPollInterval(String maxPollInterval) {
		enableMaxPoll = true;
		this.maxPollInterval = maxPollInterval;
	}

	public boolean isEnableAutoCommit() {
		return enableAutoCommit;
	}

	public void setEnableAutoCommit(boolean enableAutoCommit) {
		this.enableAutoCommit = enableAutoCommit;
	}

	public String getAutoCommitInterval() {
		return autoCommitInterval;
	}

	public void setAutoCommitInterval(String autoCommitInterval) {
		enableAutoCommit = true;
		this.autoCommitInterval = autoCommitInterval;
	}

	public boolean isEnableFetchParameter() {
		return enableFetchParameter;
	}

	public String getFetchMinBytes() {
		return fetchMinBytes;
	}

	public void setFetchMinBytes(String fetchMinBytes) {
		enableFetchParameter = true;
		this.fetchMinBytes = fetchMinBytes;
	}

	public String getFetchMaxBytes() {
		return fetchMaxBytes;
	}

	public void setFetchMaxBytes(String fetchMaxBytes) {
		enableFetchParameter = true;
		this.fetchMaxBytes = fetchMaxBytes;
	}

	public String getFetchMaxWait() {
		return fetchMaxWait;
	}

	public void setFetchMaxWait(String fetchMaxWait) {
		enableIOBehavior = true;
		this.fetchMaxWait = fetchMaxWait;
	}

	public boolean isEnableSessionConfig() {
		return enableSessionConfig;
	}

	public String getSessionTimeout() {
		return sessionTimeout;
	}

	public void setSessionTimeout(String sessionTimeout) {
		enableSessionConfig = true;
		this.sessionTimeout = sessionTimeout;
	}

	public String getHeartbeatInterval() {
		return heartbeatInterval;
	}

	public void setHeartbeatInterval(String heartbeatInterval) {
		enableSessionConfig = true;
		this.heartbeatInterval = heartbeatInterval;
	}

	public String getTopics() {
		return topics;
	}

	public void setTopics(String topics) {
		this.topics = topics;
	}

	@Override
	public void cleanUpResource() {
	}

	@Override
	public Object applyVariableSubstitution(Message messageRequest, Message messageResponse, VariableDefinitions variableDefinitions, VariableValues varValues) throws Exception {
		ConsumerConfigBuilder newConsumer = new ConsumerConfigBuilder();
		super.applyVariableSubstitution(newConsumer, messageRequest, messageResponse, variableDefinitions, varValues);
		newConsumer.setPollTimeOut(VariableSubstitution.processVariableSubstitution(pollTimeOut, messageRequest, messageResponse, variableDefinitions, varValues));
		newConsumer.setGroupId(VariableSubstitution.processVariableSubstitution(groupId, messageRequest, messageResponse, variableDefinitions, varValues));
		newConsumer.setMaxPollRecords(VariableSubstitution.processVariableSubstitution(maxPollRecords, messageRequest, messageResponse, variableDefinitions, varValues));
		newConsumer.setMaxPollInterval(VariableSubstitution.processVariableSubstitution(maxPollInterval, messageRequest, messageResponse, variableDefinitions, varValues));
		newConsumer.setEnableAutoCommit(enableAutoCommit);
		newConsumer.setAutoCommitInterval(VariableSubstitution.processVariableSubstitution(autoCommitInterval, messageRequest, messageResponse, variableDefinitions, varValues));
		newConsumer.setFetchMinBytes(VariableSubstitution.processVariableSubstitution(fetchMinBytes, messageRequest, messageResponse, variableDefinitions, varValues));
		newConsumer.setFetchMaxBytes(VariableSubstitution.processVariableSubstitution(fetchMaxBytes, messageRequest, messageResponse, variableDefinitions, varValues));
		newConsumer.setFetchMaxWait(VariableSubstitution.processVariableSubstitution(fetchMaxWait, messageRequest, messageResponse, variableDefinitions, varValues));
		newConsumer.setSessionTimeout(VariableSubstitution.processVariableSubstitution(sessionTimeout, messageRequest, messageResponse, variableDefinitions, varValues));
		newConsumer.setHeartbeatInterval(VariableSubstitution.processVariableSubstitution(heartbeatInterval, messageRequest, messageResponse, variableDefinitions, varValues));

		newConsumer.setTopics(VariableSubstitution.processVariableSubstitution(topics, messageRequest, messageResponse, variableDefinitions, varValues));

		// if (otherAdvantcedConfig != null) {
		// Iterator<NameValuePair> it = otherAdvantcedConfig.iterator();
		// while (it.hasNext()) {
		// NameValuePair oldConfig = (NameValuePair) it.next();
		// newConsumer.addOtherAdvantcedConfig(VariableSubstitution.processVariableSubstitution(oldConfig.getName(),
		// messageRequest, messageResponse, variableDefinitions, varValues),
		// VariableSubstitution.processVariableSubstitution(oldConfig.getValue(),
		// messageRequest, messageResponse, variableDefinitions, varValues));
		// }
		// }
		return newConsumer;
	}

}

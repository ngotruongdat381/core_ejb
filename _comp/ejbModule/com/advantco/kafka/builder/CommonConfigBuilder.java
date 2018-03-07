package com.advantco.kafka.builder;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.CommonClientConfigs;

import com.advantco.base.StringUtil;
import com.advantco.base.variablesubstitution.VariableDefinitions;
import com.advantco.base.variablesubstitution.VariableSubstitution;
import com.advantco.base.variablesubstitution.VariableValues;
import com.advantco.kafka.exception.KBaseException;
import com.advantco.kafka.utils.GeneralHelper;
import com.sap.engine.interfaces.messaging.api.Message;

public class CommonConfigBuilder implements IConfigBuilder {
	public static final Set<String> KNOWN_COMMON_CONFIGS;
	protected String kafkaBrokers;
	protected String requestTimeoutMs;
	protected String reconnectBackoffMaxMs;
	protected String reconnectBackoffMs;
	protected String clientId;

	protected boolean enableIOBehavior;
	protected String receiveBuffer;
	protected String sendBuffer;
	// protected Collection<NameValuePair> otherAdvancedConfig;

	// Other parameters are not in KAFKA Configuration;
	protected String connectionKeepAlive;

	static {
		Set<String> consConfigs = new HashSet<String>();
		consConfigs.add(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
		consConfigs.add(CommonClientConfigs.CLIENT_ID_CONFIG);
		consConfigs.add(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG);
		consConfigs.add(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG);
		consConfigs.add(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG);
		consConfigs.add(CommonClientConfigs.RECEIVE_BUFFER_CONFIG);
		consConfigs.add(CommonClientConfigs.SEND_BUFFER_CONFIG);
		KNOWN_COMMON_CONFIGS = Collections.unmodifiableSet(consConfigs);
	}

	CommonConfigBuilder() {
		kafkaBrokers = null;
		requestTimeoutMs = null;
		reconnectBackoffMaxMs = null;
		reconnectBackoffMs = null;
		clientId = null;
		enableIOBehavior = false;
		receiveBuffer = null;
		sendBuffer = null;
		// otherAdvancedConfig = null;
		connectionKeepAlive = null;
	}

	CommonConfigBuilder(String broker, String reqTime, String backOffMax, String backOff, String client) {
		kafkaBrokers = broker;
		requestTimeoutMs = reqTime;
		reconnectBackoffMaxMs = backOffMax;
		reconnectBackoffMs = backOff;
		clientId = client;
		enableIOBehavior = false;
		receiveBuffer = null;
		sendBuffer = null;
		// otherAdvancedConfig = null;
		connectionKeepAlive = null;
	}

	CommonConfigBuilder(String broker, String reqTime, String backOffMax, String backOff, String client, String recvBuffer, String sendBuff) {
		kafkaBrokers = broker;
		requestTimeoutMs = reqTime;
		reconnectBackoffMaxMs = backOffMax;
		reconnectBackoffMs = backOff;
		clientId = client;
		if (StringUtil.notNullNorBlank(receiveBuffer) || StringUtil.notNullNorBlank(sendBuffer)) {
			enableIOBehavior = true;
		} else {
			enableIOBehavior = false;
		}
		receiveBuffer = recvBuffer;
		sendBuffer = sendBuff;
		// otherAdvancedConfig = null;
		connectionKeepAlive = null;
	}

	@Override
	public Properties buildConfigProperties() throws KBaseException {
		return buildConfigProperties(new Properties());
	}

	@Override
	public Properties buildConfigProperties(Properties properties) throws KBaseException {
		checkAndSetDefaultValue();

		if (kafkaBrokers != null) {
			properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
		}
		if (requestTimeoutMs != null) {
			properties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
		}
		if (reconnectBackoffMaxMs != null) {
			properties.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, reconnectBackoffMaxMs);
		}
		if (reconnectBackoffMs != null) {
			properties.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, reconnectBackoffMs);
		}

		if (StringUtil.notNullNorBlank(clientId)) {
			properties.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
		} else {
			properties.put(CommonClientConfigs.CLIENT_ID_CONFIG, GeneralHelper.getClientId());
		}

		if (enableIOBehavior) {
			if (receiveBuffer != null) {
				properties.put(CommonClientConfigs.RECEIVE_BUFFER_CONFIG, receiveBuffer);
			}
			if (sendBuffer != null) {
				properties.put(CommonClientConfigs.SEND_BUFFER_CONFIG, sendBuffer);
			}
		}

		return properties;
	}

	private void checkAndSetDefaultValue() {
		if (!KConfigContext.isValidNumericValue(requestTimeoutMs)) {
			requestTimeoutMs = "305000";
		}
		if (!KConfigContext.isValidNumericValue(reconnectBackoffMaxMs)) {
			reconnectBackoffMaxMs = "1000";
		}
		if (!KConfigContext.isValidNumericValue(reconnectBackoffMs)) {
			reconnectBackoffMs = "50";
		}
		if (enableIOBehavior) {
			if (!KConfigContext.isValidNumericValue(receiveBuffer)) {
				receiveBuffer = "65536";
			}
			if (!KConfigContext.isValidNumericValue(sendBuffer)) {
				sendBuffer = "131072";
			}
		}
	}

	public String getKafkaBrokers() {
		return kafkaBrokers;
	}

	public void setKafkaBrokers(String kafkaBrokers) {
		this.kafkaBrokers = kafkaBrokers;
	}

	public String getRequestTimeoutMs() {
		return requestTimeoutMs;
	}

	public void setRequestTimeoutMs(String requestTimeoutMs) {
		this.requestTimeoutMs = requestTimeoutMs;
	}

	public String getReconnectBackoffMaxMs() {
		return reconnectBackoffMaxMs;
	}

	public void setReconnectBackoffMaxMs(String reconnectBackoffMaxMs) {
		this.reconnectBackoffMaxMs = reconnectBackoffMaxMs;
	}

	public String getReconnectBackoffMs() {
		return reconnectBackoffMs;
	}

	public void setReconnectBackoffMs(String reconnectBackoffMs) {
		this.reconnectBackoffMs = reconnectBackoffMs;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getReceiveBuffer() {
		return receiveBuffer;
	}

	public void setReceiveBuffer(String receiveBuffer) {
		enableIOBehavior = true;
		this.receiveBuffer = receiveBuffer;
	}

	public String getSendBuffer() {
		return sendBuffer;
	}

	public void setSendBuffer(String sendBuffer) {
		enableIOBehavior = true;
		this.sendBuffer = sendBuffer;
	}

	// public Collection<NameValuePair> getotherAdvancedConfig() {
	// return otherAdvancedConfig;
	// }
	//
	// public void addotherAdvancedConfig(String key, String value) {
	// if (StringUtil.notNullNorBlank(key)) {
	// if (this.otherAdvancedConfig == null) {
	// this.otherAdvancedConfig = new ArrayList<NameValuePair>();
	// }
	// this.otherAdvancedConfig.add(new NameValuePair(key, value));
	// }
	// }
	//
	// public void setotherAdvancedConfig(Collection<NameValuePair>
	// otherAdvancedConfig) {
	// this.otherAdvancedConfig = otherAdvancedConfig;
	// }

	public String getConnectionKeepAlive() {
		return connectionKeepAlive;
	}

	public void setConnectionKeepAlive(String connectionKeepAlive) {
		if (KConfigContext.isValidNumericValue(connectionKeepAlive) || connectionKeepAlive.contains(VariableSubstitution.VARIABLE_DELIMITER)) {
			this.connectionKeepAlive = connectionKeepAlive;
		} else {
			this.connectionKeepAlive = "0";
		}
	}

	@Override
	public void cleanUpResource() {
	}

	@Override
	public Object applyVariableSubstitution(Message messageRequest, Message messageResponse, VariableDefinitions variableDefinitions, VariableValues varValues) throws Exception {
		CommonConfigBuilder newCommon = new CommonConfigBuilder();
		newCommon.setKafkaBrokers(VariableSubstitution.processVariableSubstitution(kafkaBrokers, messageRequest, messageResponse, variableDefinitions, varValues));
		newCommon.setRequestTimeoutMs(VariableSubstitution.processVariableSubstitution(requestTimeoutMs, messageRequest, messageResponse, variableDefinitions, varValues));
		newCommon.setReconnectBackoffMaxMs(VariableSubstitution.processVariableSubstitution(reconnectBackoffMaxMs, messageRequest, messageResponse, variableDefinitions, varValues));
		newCommon.setReconnectBackoffMs(VariableSubstitution.processVariableSubstitution(reconnectBackoffMs, messageRequest, messageResponse, variableDefinitions, varValues));
		newCommon.setClientId(VariableSubstitution.processVariableSubstitution(clientId, messageRequest, messageResponse, variableDefinitions, varValues));
		newCommon.setReceiveBuffer(VariableSubstitution.processVariableSubstitution(receiveBuffer, messageRequest, messageResponse, variableDefinitions, varValues));
		newCommon.setSendBuffer(VariableSubstitution.processVariableSubstitution(sendBuffer, messageRequest, messageResponse, variableDefinitions, varValues));
		newCommon.setConnectionKeepAlive(VariableSubstitution.processVariableSubstitution(connectionKeepAlive, messageRequest, messageResponse, variableDefinitions, varValues));

		return newCommon;
	}

	public void applyVariableSubstitution(CommonConfigBuilder newCommon, Message messageRequest, Message messageResponse, VariableDefinitions variableDefinitions, VariableValues varValues) throws Exception {
		newCommon.setKafkaBrokers(VariableSubstitution.processVariableSubstitution(kafkaBrokers, messageRequest, messageResponse, variableDefinitions, varValues));
		newCommon.setRequestTimeoutMs(VariableSubstitution.processVariableSubstitution(requestTimeoutMs, messageRequest, messageResponse, variableDefinitions, varValues));
		newCommon.setReconnectBackoffMaxMs(VariableSubstitution.processVariableSubstitution(reconnectBackoffMaxMs, messageRequest, messageResponse, variableDefinitions, varValues));
		newCommon.setReconnectBackoffMs(VariableSubstitution.processVariableSubstitution(reconnectBackoffMs, messageRequest, messageResponse, variableDefinitions, varValues));
		newCommon.setClientId(VariableSubstitution.processVariableSubstitution(clientId, messageRequest, messageResponse, variableDefinitions, varValues));
		newCommon.setReceiveBuffer(VariableSubstitution.processVariableSubstitution(receiveBuffer, messageRequest, messageResponse, variableDefinitions, varValues));
		newCommon.setSendBuffer(VariableSubstitution.processVariableSubstitution(sendBuffer, messageRequest, messageResponse, variableDefinitions, varValues));
		newCommon.setConnectionKeepAlive(VariableSubstitution.processVariableSubstitution(connectionKeepAlive, messageRequest, messageResponse, variableDefinitions, varValues));
	}
}

package com.advantco.kafka.builder;

import java.util.Properties;

import org.apache.kafka.streams.StreamsConfig;

import com.advantco.base.StringUtil;
import com.advantco.base.variablesubstitution.VariableDefinitions;
import com.advantco.base.variablesubstitution.VariableValues;
import com.advantco.kafka.exception.KBaseException;
import com.sap.engine.interfaces.messaging.api.Message;

public class StreamConfigBuilder implements IConfigBuilder {
	private String kafkaBrokers;
	private String commandTopic;
	private String applicationId;

	StreamConfigBuilder() {
		this.kafkaBrokers = null;
		this.commandTopic = null;
		this.applicationId = null;
	}
	
	@Override
	public Properties buildConfigProperties() throws KBaseException {
		return buildConfigProperties(new Properties());
	}

	@Override
	public Properties buildConfigProperties(Properties properties) throws KBaseException {
		if (kafkaBrokers != null) {
			properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
		}
		if (applicationId != null) {
			properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		}
		return null;
	}

	public String getKafkaBrokers() {
		return kafkaBrokers;
	}

	public void setKafkaBrokers(String kafkaBrokers) {
		this.kafkaBrokers = kafkaBrokers;
	}

	public String getCommandTopic() {
		return commandTopic;
	}

	public void setCommandTopic(String commandTopic) {
		if (StringUtil.notNullNorBlank(commandTopic)) {
			this.commandTopic = commandTopic;
		} else {
			this.commandTopic = "ksql__commands";
		}
	}

	public String getApplicationId() {
		return applicationId;
	}

	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
	}

	@Override
	public void cleanUpResource() {
		// TODO Auto-generated method stub

	}

	@Override
	public Object applyVariableSubstitution(Message messageRequest, Message messageResponse, VariableDefinitions variableDefinitions, VariableValues varValues) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}

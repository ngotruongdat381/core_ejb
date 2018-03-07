package com.advantco.kafka.builder;

import java.util.Properties;

import com.advantco.base.variablesubstitution.VariableDefinitions;
import com.advantco.base.variablesubstitution.VariableValues;
import com.advantco.kafka.exception.KBaseException;
import com.sap.engine.interfaces.messaging.api.Message;

public interface IConfigBuilder {
	public Properties buildConfigProperties() throws KBaseException;

	public Properties buildConfigProperties(Properties properties) throws KBaseException;

	public void cleanUpResource();

	public Object applyVariableSubstitution(Message messageRequest, Message messageResponse, VariableDefinitions variableDefinitions, VariableValues varValues) throws Exception;
}

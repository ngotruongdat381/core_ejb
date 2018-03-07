package com.advantco.kafka.varsubstitution;

import com.advantco.base.variablesubstitution.VariableDefinitions;
import com.advantco.base.variablesubstitution.VariableValues;
import com.sap.engine.interfaces.messaging.api.Message;

public interface IVariableSubstitution {
	void resolveVarSubstitution(Message messageRequest, Message messageResponse, VariableDefinitions variableDefinitions, VariableValues varValues) throws Exception;
}
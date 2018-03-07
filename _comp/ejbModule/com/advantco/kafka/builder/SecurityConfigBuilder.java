package com.advantco.kafka.builder;

import java.io.File;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import com.advantco.base.SSLAuth;
import com.advantco.base.StringUtil;
import com.advantco.base.variablesubstitution.VariableDefinitions;
import com.advantco.base.variablesubstitution.VariableSubstitution;
import com.advantco.base.variablesubstitution.VariableValues;
import com.advantco.kafka.auth.IAuth;
import com.advantco.kafka.auth.KSSLAuth;
import com.advantco.kafka.auth.KSaslPlainAuth;
import com.advantco.kafka.auth.KSaslScramAuth;
import com.advantco.kafka.exception.KBaseException;
import com.sap.engine.interfaces.messaging.api.Message;

public class SecurityConfigBuilder implements IConfigBuilder {
	public static final String AUTH_NONE = "NONE";
	private String user;
	private String pass;
	private SSLAuth sslAuth;
	private String mechanism;
	IAuth auth = null;
	IAuth authSSL = null;

	SecurityConfigBuilder() {
		this.mechanism = null;
		this.user = null;
		this.pass = null;
		this.sslAuth = null;
	}

	SecurityConfigBuilder(String mechanism, String user, String pass, SSLAuth sslAuth) {
		this.mechanism = mechanism;
		this.user = user;
		this.pass = pass;
		this.sslAuth = sslAuth;
	}

	@Override
	public Properties buildConfigProperties() throws KBaseException {
		return buildConfigProperties(new Properties());
	}

	@Override
	public Properties buildConfigProperties(Properties properties) throws KBaseException {
		if (AUTH_NONE.equals(getAuthMechanism(mechanism))) {
			properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name());
			return properties;
		}

		if (sslAuth == null) {
			if (KSaslPlainAuth.PLAIN.equals(getAuthMechanism(mechanism))) {
				authSSL = new KSaslPlainAuth(user, pass);
				authSSL.saslAuthBuildConfig(properties);
				properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
			} else if (KSaslScramAuth.SCRAM_SHA_256.equals(getAuthMechanism(mechanism)) || KSaslScramAuth.SCRAM_SHA_512.equals(getAuthMechanism(mechanism))) {
				throw new KBaseException("Invalid SSL with SCRAM SHA 256/512");
			}
		} else if (StringUtil.nullOrBlank(user)) {
			authSSL = new KSSLAuth(sslAuth);
			authSSL.saslAuthBuildConfig(properties);
			properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
		} else {
			authSSL = new KSSLAuth(sslAuth);
			authSSL.saslAuthBuildConfig(properties);
			if (KSaslScramAuth.SCRAM_SHA_256.equals(getAuthMechanism(mechanism))) {
				auth = new KSaslScramAuth(user, pass, true);
			} else if (KSaslScramAuth.SCRAM_SHA_512.equals(getAuthMechanism(mechanism))) {
				auth = new KSaslScramAuth(user, pass, false);
			} else {
				auth = new KSaslPlainAuth(user, pass);
			}
			auth.saslAuthBuildConfig(properties);
			properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name());
		}

		return properties;
	}

	public String getAuthMechanism(String mechanism) {
		String result = null;
		if (null == mechanism || "none".equalsIgnoreCase(mechanism)) {
			result = AUTH_NONE;
		} else if ("plain".equalsIgnoreCase(mechanism)) {
			result = KSaslPlainAuth.PLAIN;
		} else if ("scram256".equalsIgnoreCase(mechanism)) {
			result = KSaslScramAuth.SCRAM_SHA_256;
		} else if ("scram512".equalsIgnoreCase(mechanism)) {
			result = KSaslScramAuth.SCRAM_SHA_512;
		} else {
			result = mechanism;
		}
		return result;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPass() {
		return pass;
	}

	public void setPass(String pass) {
		this.pass = pass;
	}

	public SSLAuth getSslAuth() {
		return sslAuth;
	}

	public void setSslAuth(SSLAuth sslAuth) {
		this.sslAuth = sslAuth;
	}

	public String getMechanism() {
		return mechanism;
	}

	public void setMechanism(String mechanism) {
		this.mechanism = mechanism;
	}

	public IAuth getAuth() {
		return auth;
	}

	public void setAuth(IAuth auth) {
		this.auth = auth;
	}

	public IAuth getAuthSSL() {
		return authSSL;
	}

	public void setAuthSSL(IAuth authSSL) {
		this.authSSL = authSSL;
	}

	@Override
	public void cleanUpResource() {
		if (sslAuth != null) {
			if ((!sslAuth.isFixedClientKeystoreFileUsed()) && ((KSSLAuth) authSSL).getRuntimeClientKeystore() != null && ((KSSLAuth) authSSL).getRuntimeClientKeystore()[0] != null) {
				new File(((KSSLAuth) authSSL).getRuntimeClientKeystore()[0]).delete();
			}
			if ((!sslAuth.isFixedServerTruststoreFileUsed()) && ((KSSLAuth) authSSL).getRuntimeBrokerTruststore() != null && ((KSSLAuth) authSSL).getRuntimeBrokerTruststore()[0] != null) {
				new File(((KSSLAuth) authSSL).getRuntimeBrokerTruststore()[0]).delete();
			}
		}
	}

	@Override
	public Object applyVariableSubstitution(Message messageRequest, Message messageResponse, VariableDefinitions variableDefinitions, VariableValues varValues) throws Exception {
		SecurityConfigBuilder newSecurity = new SecurityConfigBuilder();

		newSecurity.setUser(VariableSubstitution.processVariableSubstitution(user, messageRequest, messageResponse, variableDefinitions, varValues));
		newSecurity.setPass(VariableSubstitution.processVariableSubstitution(pass, messageRequest, messageResponse, variableDefinitions, varValues));
		newSecurity.setMechanism(mechanism);
		if (sslAuth != null) {
			SSLAuth newsslAuth = sslAuth.applyVariableSubstitution(variableDefinitions, varValues, VariableSubstitution.convert(messageRequest), VariableSubstitution.convert(messageResponse));
			newSecurity.setSslAuth(newsslAuth);
		}
		return newSecurity;
	}

}

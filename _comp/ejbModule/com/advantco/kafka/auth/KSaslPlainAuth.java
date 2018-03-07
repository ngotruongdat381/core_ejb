package com.advantco.kafka.auth;

import java.util.Properties;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.plain.PlainSaslServer;

import com.advantco.kafka.exception.InvalidAuthException;

public class KSaslPlainAuth extends KSaslAuth {
	public final static String PLAIN = PlainSaslServer.PLAIN_MECHANISM;

	public KSaslPlainAuth(String username, String password) {
		super(username, password);
	}

	@Override
	public Properties saslAuthBuildConfig(Properties properties) throws InvalidAuthException {
		properties.put(SaslConfigs.SASL_MECHANISM, PLAIN);
		properties.put(SaslConfigs.SASL_JAAS_CONFIG, buildJassConfig(PlainLoginModule.class.getName()));
		return properties;
	}

}

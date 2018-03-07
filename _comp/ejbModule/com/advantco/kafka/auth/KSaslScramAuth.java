package com.advantco.kafka.auth;

import java.util.Properties;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.scram.ScramMechanism;

import com.advantco.kafka.exception.InvalidAuthException;

public class KSaslScramAuth extends KSaslAuth {
	public final static String SCRAM_SHA_256 = ScramMechanism.SCRAM_SHA_256.mechanismName();
	public final static String SCRAM_SHA_512 = ScramMechanism.SCRAM_SHA_512.mechanismName();

	private String hashAlgorithm;

	public KSaslScramAuth(String username, String password, boolean  is256) {
		super(username, password);
		if (is256) {
			this.hashAlgorithm = SCRAM_SHA_256;
		} else {
			this.hashAlgorithm = SCRAM_SHA_512;
		}
	}

	public KSaslScramAuth(String username, String password, String hashAlgorithm) {
		super(username, password);
		this.hashAlgorithm = hashAlgorithm;
	}

	@Override
	public Properties saslAuthBuildConfig(Properties properties) throws InvalidAuthException {
		properties.put(SaslConfigs.SASL_MECHANISM, hashAlgorithm);
		properties.put(SaslConfigs.SASL_JAAS_CONFIG, buildJassConfig(ScramLoginModule.class.getName()));
		return properties;
	}

}

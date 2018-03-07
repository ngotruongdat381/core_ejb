package com.advantco.kafka.auth;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;

import com.advantco.base.SSLAuth;
import com.advantco.kafka.exception.InvalidAuthException;

public class KSSLAuth implements IAuth {
	protected SSLAuth sslAuth;
	private volatile String[] runtimeClientKeystore;
	private volatile String[] runtimeBrokerTruststore;

	public KSSLAuth(SSLAuth sslAuth) {
		this.sslAuth = sslAuth;
	}

	@Override
	public Properties saslAuthBuildConfig(Properties properties) throws InvalidAuthException {
		try {
			runtimeClientKeystore = sslAuth.getRuntimeClientKeystoreFile();
			runtimeBrokerTruststore = sslAuth.getRuntimeServerTruststoreFile();
		} catch (IOException | GeneralSecurityException ex) {
			throw new InvalidAuthException(ex.getMessage(), ex);
		}

		if (sslAuth != null) {
			if (null != runtimeClientKeystore[0] && null != runtimeClientKeystore[1] && null != runtimeClientKeystore[2]) {
				properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
				properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, runtimeClientKeystore[0]);
				properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, runtimeClientKeystore[1]);
				properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, runtimeClientKeystore[2]);
			}
			if (null != runtimeBrokerTruststore[0] && null != runtimeBrokerTruststore[1] && null != runtimeBrokerTruststore[2]) {
				properties.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS);
				properties.put(SslConfigs.SSL_PROTOCOL_CONFIG, SslConfigs.DEFAULT_SSL_PROTOCOL);
				properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, runtimeBrokerTruststore[0]);
				properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, runtimeBrokerTruststore[1]);
				properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, runtimeBrokerTruststore[2]);
			}
		}
		return properties;

	}

	public String[] getRuntimeClientKeystore() {
		return runtimeClientKeystore;
	}

	public String[] getRuntimeBrokerTruststore() {
		return runtimeBrokerTruststore;
	}

}

package com.advantco.kafka.auth;

import java.util.Properties;

import com.advantco.kafka.exception.InvalidAuthException;

public interface IAuth {
	public Properties saslAuthBuildConfig(Properties properties) throws InvalidAuthException;

}

package com.advantco.kafka.auth;

public abstract class KSaslAuth implements IAuth {

	protected String username;
	protected String password;

	public KSaslAuth(String username, String password) {
		super();
		this.username = username;
		this.password = password;
	}

	protected String buildJassConfig(String loginClassImpl) {
		StringBuilder builder = new StringBuilder();
		builder.append(loginClassImpl);
		builder.append(' ');
		builder.append("required");
		builder.append(' ');
		builder.append("username");
		builder.append('=');
		builder.append(username);
		builder.append(' ');
		builder.append("password");
		builder.append('=');
		builder.append(password);
		builder.append(';');
		return builder.toString();
	}

}

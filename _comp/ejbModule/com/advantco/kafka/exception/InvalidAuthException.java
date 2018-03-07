package com.advantco.kafka.exception;

public class InvalidAuthException extends KBaseException {
	private static final long serialVersionUID = 1L;

	public InvalidAuthException() {
		super();
	}

	public InvalidAuthException(String message) {
		super(message);
	}

	public InvalidAuthException(Throwable cause) {
		super(cause);
	}

	public InvalidAuthException(Exception ex) {
		super(ex);
	}

	public InvalidAuthException(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidAuthException(String message, Exception ex) {
		super(message, ex);
	}
}

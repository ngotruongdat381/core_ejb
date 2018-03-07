package com.advantco.kafka.exception;

public class KBaseException extends Exception {

	private static final long serialVersionUID = 1L;

	public KBaseException() {
		super();
	}

	public KBaseException(String message) {
		super(message);
	}

	public KBaseException(Throwable cause) {
		super(cause);
	}

	public KBaseException(Exception ex) {
		super(ex);
	}

	public KBaseException(String message, Throwable cause) {
		super(message, cause);
	}

	public KBaseException(String message, Exception ex) {
		super(message, ex);
	}

}
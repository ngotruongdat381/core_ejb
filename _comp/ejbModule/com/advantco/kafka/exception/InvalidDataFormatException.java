package com.advantco.kafka.exception;

public class InvalidDataFormatException extends KBaseException {
	private static final long serialVersionUID = 1L;

	public InvalidDataFormatException() {
		super();
	}

	public InvalidDataFormatException(String message) {
		super(message);
	}

	public InvalidDataFormatException(Throwable cause) {
		super(cause);
	}

	public InvalidDataFormatException(Exception ex) {
		super(ex);
	}

	public InvalidDataFormatException(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidDataFormatException(String message, Exception ex) {
		super(message, ex);
	}
}

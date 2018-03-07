package com.advantco.kafka.format;

import com.advantco.kafka.exception.InvalidDataFormatException;

public interface AdvDataFormat {

	public String getMechanism();

	public String getValue() throws InvalidDataFormatException;

}

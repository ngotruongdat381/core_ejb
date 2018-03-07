package com.advantco.kafka.format.avro;

import java.io.IOException;

import com.advantco.base.IOUtil;
import com.advantco.kafka.exception.InvalidDataFormatException;
import com.advantco.kafka.format.AdvDataFormat;

public class AdvArvoDataFormat implements AdvDataFormat {

	public static final String AVROFORMAT = "AVRO";

	public enum AvroSchemaType {
		TEXT, FILE_SYSTEM
	}

	private AvroSchemaType avroSchemaType;
	private String schemaInJson;
	private String schemaFromFile;

	public AvroSchemaType getAvroSchemaType() {
		return avroSchemaType;
	}

	public void setAvroSchemaType(AvroSchemaType avroSchemaType) {
		this.avroSchemaType = avroSchemaType;
	}

	public String getSchemaInJson() {
		return schemaInJson;
	}

	public void setSchemaInJson(String schemaInJson) {
		this.schemaInJson = schemaInJson;
	}

	public String getSchemaFromFile() {
		return schemaFromFile;
	}

	public void setSchemaFromFile(String schemaFromFile) {
		this.schemaFromFile = schemaFromFile;
	}

	@Override
	public String getMechanism() {
		return AVROFORMAT;
	}

	@Override
	public String getValue() throws InvalidDataFormatException {
		try {
			if (AvroSchemaType.TEXT.equals(avroSchemaType)) {
				return schemaInJson;
			} else if (AvroSchemaType.FILE_SYSTEM.equals(avroSchemaType)) {
				byte[] bytes = IOUtil.read(schemaFromFile);
				return new String(bytes, "UTF-8");
			}
		} catch (IOException ex) {
			throw new InvalidDataFormatException(ex.getMessage(), ex);
		}
		return null;
	}
}

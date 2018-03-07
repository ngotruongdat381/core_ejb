package com.advantco.kafka.format.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.advantco.kafka.builder.DataFormatBuilder;

public class KAvroSerializer implements Serializer<byte[]> {

	private final static EncoderFactory ENCODER_FACTORY = EncoderFactory.get();
	private final static DecoderFactory DECODER_FACTORY = DecoderFactory.get();

	private Schema jsonSchema;

	public KAvroSerializer() {
		super();
	}

	@Override
	public void close() {
		// Not support

	}

	@Override
	public void configure(Map<String, ?> configs, boolean arg1) {
		jsonSchema = new Schema.Parser().parse((String) configs.get(DataFormatBuilder.ADV_AVRO_SCHEMA_VALUE));
	}

	@Override
	public byte[] serialize(String topic, byte[] jsonData) {
		return serializeImpl(jsonData);
	}

	protected byte[] serializeImpl(byte[] jsonData) throws SerializationException {
		try {
			DatumReader<Object> reader = new GenericDatumReader<Object>(jsonSchema);
			Object avroObject = reader.read(null, DECODER_FACTORY.jsonDecoder(jsonSchema, new String(jsonData, "UTF-8")));

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			BinaryEncoder encoder = ENCODER_FACTORY.binaryEncoder(baos, null);

			DatumWriter<Object> writer = new GenericDatumWriter<Object>(jsonSchema);
			writer.write(avroObject, encoder);
			encoder.flush();
			return baos.toByteArray();
		} catch (IOException e) {
			throw new SerializationException(String.format("Error when serializing json %s to byte[] of schema %s", new String(jsonData), jsonSchema), e);
		} catch (AvroRuntimeException e) {
			throw new SerializationException(String.format("Error when serializing json %s to byte[] of schema %s", new String(jsonData), jsonSchema), e);
		}
	}

}
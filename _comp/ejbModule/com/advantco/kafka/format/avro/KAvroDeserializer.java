package com.advantco.kafka.format.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.advantco.kafka.builder.DataFormatBuilder;

public class KAvroDeserializer implements Deserializer<byte[]> {

	private final static EncoderFactory ENCODER_FACTORY = EncoderFactory.get();
	private final static DecoderFactory DECODER_FACTORY = DecoderFactory.get();

	private Schema jsonSchema;

	public KAvroDeserializer() {
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
	public byte[] deserialize(String topic, byte[] avroData) {
		if (avroData == null) {
			return null;
		}
		try {
			DatumReader<Object> reader = new GenericDatumReader<Object>(jsonSchema);
			Object avroObject = reader.read(null, DECODER_FACTORY.binaryDecoder(avroData, null));

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			JsonEncoder encoder = ENCODER_FACTORY.jsonEncoder(jsonSchema, baos);

			DatumWriter<Object> writer = new GenericDatumWriter<Object>(jsonSchema);
			writer.write(avroObject, encoder);
			encoder.flush();
			return baos.toByteArray();
		} catch (IOException e) {
			throw new SerializationException("Error when deserializing byte[] to Avro message", e);
		}
	}

}

package com.deviantart.kafka_connect_s3;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

/**
 * Encodes raw bytes, prefixed by a 4 byte, big-endian integer
 * indicating the length of the byte sequence.
 */
public class ByteLengthEncodedConverter implements Converter {

	private static final int LEN_SIZE = 4;
	// need to write something for null, so we write just 0
	private static final byte[] NULL = new byte[]{0, 0, 0, 0};

	private Converter subConverter;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// allow another converter to get the raw bytes
		subConverter = S3SinkTask.buildConverter(configs, "converter", isKey,
				AlreadyBytesConverter.class);
	}

	public static class AlreadyBytesConverter implements Converter {

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
		}

		@Override
		public byte[] fromConnectData(String topic, Schema schema, Object value) {
			if (schema != Schema.BYTES_SCHEMA && schema != Schema.OPTIONAL_BYTES_SCHEMA) {
				throw new DataException(topic + " error: Not a byte array! " + value);
			}
			if (value == null) {
				return NULL;
			}
			return (byte[]) value;
		}

		@Override
		public SchemaAndValue toConnectData(String topic, byte[] value) {
			throw new UnsupportedOperationException("This converter is not intended for deserializing records!");
		}
	}

	/**
	 * Converts Connect data to a length prefixed encoded byte array.
	 *
	 * With respect to the S3 Sink, this method determines the raw bytes written to S3.
	 */
	@Override
	public byte[] fromConnectData(String topic, Schema schema, Object value) {
		final byte[] bytes = subConverter.fromConnectData(topic, schema, value);
		int len = bytes.length;
		byte[] result = new byte[LEN_SIZE + len];
		ByteBuffer.wrap(result).putInt(len);
		System.arraycopy(bytes, 0, result, LEN_SIZE, len);
		return result;
	}

	/**
	 * No conversion. Returns the raw bytes. Makes no assumptions about format.
	 * Does NOT decode byte length, because this data is presumably from
	 * a raw record, not S3.
	 *
	 * With respect to the Sink, this method will determine the format of the
	 * data pulled off of the subscribed topic.
	 */
	@Override
	public SchemaAndValue toConnectData(String topic, byte[] value) {
		return new SchemaAndValue(
				Schema.BYTES_SCHEMA,
				value
		);
	}
}

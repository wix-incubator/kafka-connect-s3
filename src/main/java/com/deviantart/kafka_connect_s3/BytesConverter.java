package com.deviantart.kafka_connect_s3;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

/**
 * The non-converter converter. Simply passes the raw bytes through.
 */
public class BytesConverter implements Converter {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public byte[] fromConnectData(String topic, Schema schema, Object value) {
		if (!(value instanceof byte[])) {
			throw new DataException("Not a byte array! " + value);
		}
		return (byte[]) value;
	}

	@Override
	public SchemaAndValue toConnectData(String topic, byte[] value) {
		return new SchemaAndValue(
				Schema.BYTES_SCHEMA,
				value
		);
	}
}

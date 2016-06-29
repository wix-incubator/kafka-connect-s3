package com.deviantart.kafka_connect_s3;

import java.nio.charset.Charset;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

/**
 * Converts data to/from a delimited, encoded string.
 * Both delimiter and encoding are configurable.
 */
public class ToStringWithDelimiterConverter implements Converter {

	private Charset encoding = Charset.forName("UTF-8");
	private String delimiter = "\n";

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		if (configs.containsKey("encoding")) {
			encoding = Charset.forName(configs.get("encoding").toString());
		}
		if (configs.containsKey("delimiter")) {
			delimiter = configs.get("delimiter").toString();
		}
	}

	// this is called internally by the Sink to get the raw bytes to write to S3
	@Override
	public byte[] fromConnectData(String topic, Schema schema, Object value) {
		if (schema != Schema.STRING_SCHEMA && schema != Schema.OPTIONAL_STRING_SCHEMA) {
			throw new IllegalArgumentException(topic + " data is not a string! " + value);
		}
		if (value == null) {
			value = "";
		}
		String s = value.toString();
		// ensure we have the delimiter
		if (!s.endsWith(delimiter)) {
			s += delimiter;
		}
		return s.getBytes(encoding);
	}

	// this is what is called by the Connect Worker before passing the data to the Sink
	@Override
	public SchemaAndValue toConnectData(String topic, byte[] value) {
		String s = new String(value, encoding);
		// if we have raw data with the delimiter already appended, remove it
		if (s.endsWith(delimiter)) {
			s = s.substring(0, s.length() - 1 - delimiter.length());
		}
		return new SchemaAndValue(
				Schema.STRING_SCHEMA,
				s
		);
	}
}

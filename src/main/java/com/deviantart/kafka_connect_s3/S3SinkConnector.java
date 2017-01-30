package com.deviantart.kafka_connect_s3;

import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * S3SinkConnector is a Kafka Connect Connector implementation that exports data from Kafka to S3.
 */
public class S3SinkConnector extends SinkConnector {

  public static final String MAX_BLOCK_SIZE_CONFIG = "compressed_block_size";

  public static final Long DEFAULT_MAX_BLOCK_SIZE = 67108864L;

  public static final String S3_BUCKET_CONFIG = "s3.bucket";

  public static final String S3_PREFIX_CONFIG = "s3.prefix";

  public static final String OVERRIDE_S3_ENDPOINT_CONFIG = "s3.endpoint";

  public static final String S3_PATHSTYLE_CONFIG = "s3.path_style";

  public static final String BUFFER_DIRECTORY_PATH_CONFIG = "local.buffer.dir";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(MAX_BLOCK_SIZE_CONFIG, Type.LONG, DEFAULT_MAX_BLOCK_SIZE, Range.atLeast(0), Importance.LOW, "Maximum size of data chunks in bytes (before compression)")
    .define(S3_BUCKET_CONFIG, Type.STRING, Importance.HIGH, "Name of the S3 bucket")
    .define(S3_PREFIX_CONFIG, Type.STRING, "", Importance.HIGH, "Path prefix of files to be written to S3")
    .define(OVERRIDE_S3_ENDPOINT_CONFIG, Type.STRING, "", Importance.LOW, "Override the S3 URL endpoint")
    .define(S3_PATHSTYLE_CONFIG, Type.BOOLEAN, false, Importance.LOW, "Override the standard S3 URL style by placing the bucket name in the path instead of hostname")
    .define(BUFFER_DIRECTORY_PATH_CONFIG, Type.STRING, Importance.HIGH, "Path to directory to store data temporarily before uploading to S3")
    ;

  private Map<String, Object> configProperties;

  @Override
  public String version() {
    return S3SinkConnectorConstants.VERSION;
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    readConfig(props);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return S3SinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> props = new HashMap<>();
      System.out.println(configProperties);
      props.put(MAX_BLOCK_SIZE_CONFIG, configProperties.get(MAX_BLOCK_SIZE_CONFIG).toString());
      props.put(S3_BUCKET_CONFIG, configProperties.get(S3_BUCKET_CONFIG).toString());
      props.put(S3_PREFIX_CONFIG, configProperties.get(S3_PREFIX_CONFIG).toString());
      props.put(OVERRIDE_S3_ENDPOINT_CONFIG, configProperties.get(OVERRIDE_S3_ENDPOINT_CONFIG).toString());
      props.put(S3_PATHSTYLE_CONFIG, configProperties.get(S3_PATHSTYLE_CONFIG).toString());
      props.put(BUFFER_DIRECTORY_PATH_CONFIG, configProperties.get(BUFFER_DIRECTORY_PATH_CONFIG).toString());
      configs.add(props);
    }
    return configs;
  }

  private void readConfig(Map<String, String> props) {
    // Validates the configuration and returns the valid config or throws an exception.
    configProperties = CONFIG_DEF.parse(props);

    String bufferDirectoryPath = configProperties.get(BUFFER_DIRECTORY_PATH_CONFIG).toString();
    if (!Files.isDirectory(Paths.get(bufferDirectoryPath))) {
      throw new ConnectException(String.format("%s not a directory or not valid: %s", BUFFER_DIRECTORY_PATH_CONFIG, bufferDirectoryPath));
    }
  }

  @Override
  public void stop() throws ConnectException {

  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }
}

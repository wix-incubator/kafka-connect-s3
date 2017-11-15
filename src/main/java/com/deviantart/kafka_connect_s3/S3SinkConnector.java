package com.deviantart.kafka_connect_s3;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * S3SinkConnector is a Kafka Connect Connector implementation that exports data from Kafka to S3.
 */
public class S3SinkConnector extends Connector {

  private Map<String, String> configProperties;

  @Override
  public String version() {
    return S3SinkConnectorConstants.VERSION;
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    configProperties = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return S3SinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    Map<String, String> taskProps = new HashMap<>();
    taskProps.putAll(configProperties);
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() throws ConnectException {

  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }
}

package com.personali.kafka.connect.cloud.storage;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CloudStorageSinkConnector is a Kafka Connect Connector implementation that exports data from Kafka to a Cloud Storage such as S3/GCS.
 */
public class CloudStorageSinkConnector extends Connector {

  private Map<String, String> configProperties;

  @Override
  public String version() {
    return CloudStorageSinkConnectorConstants.VERSION;
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    configProperties = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return CloudStorageSinkTask.class;
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

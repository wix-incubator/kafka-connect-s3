package com.deviantart.kafka_connect_s3;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class S3SinkTask extends SinkTask {

  private Map<String, String> config;

  public S3SinkTask() {

  }

  @Override
  public String version() {
    return S3SinkConnectorConstants.VERSION;
  }

  @Override
  public void start(Map<String, String> props) {
    config = props;
  }

  @Override
  public void stop() throws ConnectException {
    // Flush and close current temp file
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    // See if we have a tmp file open for this TopicPartition
    // if not open one
    // Write to GZIPOutputStream for current file
    // increment counter for current chunk bytes based on length of each record
    // if we have written more than CHUNK_THRESHOLD uncompressed bytes, rotate
    // to new output file
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // Foreach topic partition:
    //   Loop through all chunk files written in order and concatenate them
    //   into final output
    //   Gather message offsets and byte offsets/sizes as we go
    //   write out index file with metadata
    //   PUT both files to S3 bucket
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

  }

  private void recover(Set<TopicPartition> assignment) {

  }
}

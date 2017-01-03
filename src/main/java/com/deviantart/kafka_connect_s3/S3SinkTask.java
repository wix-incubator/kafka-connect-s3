package com.deviantart.kafka_connect_s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.deviantart.kafka_connect_s3.flush.FlushAdditionalTask;
import com.deviantart.kafka_connect_s3.partition.S3Partition;
import com.deviantart.kafka_connect_s3.partition.TopicPartitionFiles;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


public class S3SinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(S3SinkTask.class);

  private Map<String, String> config;

  private Map<TopicPartition, TopicPartitionFiles> topicPartitionFilesMap;

  private long GZIPChunkThreshold = 67108864;

  private S3Writer s3;

  public S3SinkTask() {
    topicPartitionFilesMap = new HashMap<>();
  }

  @Override
  public String version() {
    return S3SinkConnectorConstants.VERSION;
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    config = props;
    String chunkThreshold = config.get("compressed_block_size");
    if (chunkThreshold != null) {
      try {
        this.GZIPChunkThreshold = Long.parseLong(chunkThreshold);
      } catch (NumberFormatException nfe) {
        // keep default
      }
    }
    String bucket = config.get("s3.bucket");
    String prefix = config.get("s3.prefix");
    if (bucket == null || bucket == "") {
      throw new ConnectException("S3 bucket must be configured");
    }
    if (prefix == null) {
      prefix = "";
    }

    // Use default credentials provider that looks in Env + Java properties + profile + instance role
    AmazonS3 s3Client = new AmazonS3Client();

    // If worker config sets explicit endpoint override (e.g. for testing) use that
    String s3Endpoint = config.get("s3.endpoint");
    if (s3Endpoint != null && s3Endpoint != "") {
      s3Client.setEndpoint(s3Endpoint);
    }
    Boolean s3PathStyle = Boolean.parseBoolean(config.get("s3.path_style"));
    if (s3PathStyle) {
      s3Client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build());
    }

    s3 = new S3Writer(bucket, props, s3Client);

    // Recover initial assignments
    Set<TopicPartition> assignment = context.assignment();
    recoverAssignment(assignment);
  }

  @Override
  public void stop() throws ConnectException {
    // We could try to be smart and flush buffer files to be resumed
    // but for now we just start again from where we got to in S3 and overwrite any
    // buffers on disk.
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    for (SinkRecord record : records) {
      try {
            String topic = record.topic();
            int partition = record.kafkaPartition();
            TopicPartition tp = new TopicPartition(topic, partition);

            //Get partitioning info from configurations
            S3Partition s3Partition = (S3Partition)Class.forName(config.get("s3.partition.class"))
                                                    .getConstructor(SinkRecord.class,Map.class)
                                                    .newInstance(record,config);

            topicPartitionFilesMap.get(tp).putRecord(s3Partition,record.kafkaOffset(),record.value().toString());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) throws ConnectException {
    // Don't rely on offsets passed. They have some quirks like including topic partitions that just
    // got revoked (i.e. we have deleted the writer already). Not sure if this is intended...
    // https://twitter.com/mr_paul_banks/status/702493772983177218

    Map<TopicPartition,ArrayList<String>> s3DataFileKeys = new HashMap<>();
    // Instead iterate over the writers we do have and get the offsets directly from them.
    for (Map.Entry<TopicPartition, TopicPartitionFiles> entry : topicPartitionFilesMap.entrySet()) {
      s3DataFileKeys.put(entry.getKey(),entry.getValue().flushFiles());
    }

    //Case there is another Task to run after uploading files to S3
    if (config.containsKey("additional.flush.task.class")){
      FlushAdditionalTask additionalTask;
      try {
        additionalTask = (FlushAdditionalTask)Class.forName(config.get("additional.flush.task.class")).newInstance();
      } catch (Exception e) {
        throw new ConnectException("Couldn't load or instantiate additional flush task class");
      }
      additionalTask.run(s3DataFileKeys,config);
    }
  }

  @Override
  public void open(Collection<TopicPartition> partitions) throws ConnectException {
    recoverAssignment(partitions);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) throws ConnectException {
    for (TopicPartition tp : partitions) {
      if (topicPartitionFilesMap.get(tp) != null) {
        topicPartitionFilesMap.get(tp).revokeFiles();
        topicPartitionFilesMap.remove(tp);
      }
    }
  }

  private void recoverAssignment(Collection<TopicPartition> partitions) throws ConnectException {
    for (TopicPartition tp : partitions) {
      // See if this is a new assignment
      if (this.topicPartitionFilesMap.get(tp) == null) {
        log.info("Assigned new partition {} creating buffer writer", tp);
        try {
          recoverPartition(tp);
        } catch (IOException ioe) {
          throw new ConnectException("Failed to resume TopicPartition from S3", ioe);
        }
      }
    }
  }

  private void recoverPartition(TopicPartition tp) throws IOException {
    this.context.pause(tp);

    // Recover last committed offset from S3
    long offset = s3.fetchOffset(tp);

    log.info("Recovering partition {} from offset {}", tp, offset);

    TopicPartitionFiles topicPartitionFiles = new TopicPartitionFiles(tp,config.get("local.buffer.dir"), this.GZIPChunkThreshold, s3);
    topicPartitionFilesMap.put(tp, topicPartitionFiles);

    this.context.offset(tp, offset);
    this.context.resume(tp);
  }
}

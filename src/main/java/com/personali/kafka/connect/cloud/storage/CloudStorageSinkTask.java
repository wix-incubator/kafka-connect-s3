package com.personali.kafka.connect.cloud.storage;

import com.personali.kafka.connect.cloud.storage.flush.FlushAdditionalTask;
import com.personali.kafka.connect.cloud.storage.partition.StoragePartition;
import com.personali.kafka.connect.cloud.storage.partition.TopicPartitionFiles;
import com.personali.kafka.connect.cloud.storage.storage.StorageWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


public class CloudStorageSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(CloudStorageSinkTask.class);

  private Map<String, String> config;

  private Map<TopicPartition, TopicPartitionFiles> topicPartitionFilesMap;

  private StorageWriter storage;

  public CloudStorageSinkTask() {
    topicPartitionFilesMap = new HashMap<>();
  }

  @Override
  public String version() {
    return CloudStorageSinkConnectorConstants.VERSION;
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    config = props;

    //Use S3 storage writer as default
    String storageClassStr = "com.personali.kafka.connect.cloud.storage.storage.S3StorageWriter";

    if (config.containsKey("storage.class")){
      storageClassStr = config.get("storage.class");
    }

    try {
      storage = (StorageWriter) Class.forName(storageClassStr)
                        .getConstructor(Map.class)
                        .newInstance(config);
    } catch (Exception e) {
      e.printStackTrace();
      throw new ConnectException("Couldn't instantiate class " +storageClassStr,e);
    }

    //Make sure configured additional flush task class is available
    if (config.containsKey("additional.flush.task.class")){
      try {
            Class.forName(config.get("additional.flush.task.class"));
        } catch (ClassNotFoundException e) {
            throw new ConnectException("Additional flush task class was not found",e);
        }
    }

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
            StoragePartition storagePartition;

            try {
              //Get partitioning info from configurations
              String partitionClass = null;
              if (config.containsKey("s3.partition.class")){
                //Support backward compatibility
                partitionClass = config.get("s3.partition.class");
              }
              else{
                partitionClass = config.get("storage.partition.class");
              }
              storagePartition = (StoragePartition) Class.forName(partitionClass)
                      .getConstructor(SinkRecord.class, Map.class)
                      .newInstance(record, config);
            }
            //Case when instantiation of the StoragePartition object is failing probably due to
            //failure parsing or deciding on record partition
            catch (InstantiationException ie){
              log.error("Could not decide on a storage partition for record: {}",record);
              storagePartition = null;
            }

            topicPartitionFilesMap.get(tp).putRecord(storagePartition, record.kafkaOffset(), record.value().toString());
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

    //Case there is another Task to run after uploading files to the storage
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
          throw new ConnectException("Failed to resume TopicPartition from Storage", ioe);
        }
      }
    }
  }


  private void recoverPartition(TopicPartition tp) throws IOException {
    this.context.pause(tp);

    // Recover last committed offset from cloud storage
    long offset = storage.fetchOffset(tp);

    log.info("Recovering partition {} from offset {}", tp, offset);

    TopicPartitionFiles topicPartitionFiles = new TopicPartitionFiles(tp,config.get("local.buffer.dir"), storage.getGZIPChunkThreshold(), storage);
    topicPartitionFilesMap.put(tp, topicPartitionFiles);

    this.context.offset(tp, offset);
    this.context.resume(tp);
  }
}

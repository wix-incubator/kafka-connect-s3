package com.deviantart.kafka_connect_s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;

import java.util.HashMap;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;


public class S3SinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(S3SinkTask.class);

  private String chunkThreshold;

  private String bucket;

  private String prefix;

  private String overrideS3Endpoint;

  private Boolean s3PathStyle;

  private String bufferDirectoryPath;

  private Map<TopicPartition, BlockGZIPFileWriter> tmpFiles;

  private Long maxBlockSize;

  private S3Writer s3;

  public S3SinkTask() {
    tmpFiles = new HashMap<>();
  }

  @Override
  public String version() {
    return S3SinkConnectorConstants.VERSION;
  }

  private void readConfig(Map<String, String> props) {
    Map<String, Object> config = S3SinkConnector.CONFIG_DEF.parse(props);

    maxBlockSize = (Long)config.get(S3SinkConnector.MAX_BLOCK_SIZE_CONFIG);
    bucket = (String)config.get(S3SinkConnector.S3_BUCKET_CONFIG);
    prefix = (String)config.get(S3SinkConnector.S3_PREFIX_CONFIG);
    overrideS3Endpoint = (String)config.get(S3SinkConnector.OVERRIDE_S3_ENDPOINT_CONFIG);
    s3PathStyle = (Boolean)config.get(S3SinkConnector.S3_PATHSTYLE_CONFIG);
    bufferDirectoryPath = (String)config.get(S3SinkConnector.BUFFER_DIRECTORY_PATH_CONFIG);
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    readConfig(props);

    // Use default credentials provider that looks in Env + Java properties + profile + instance role
    AmazonS3 s3Client = new AmazonS3Client();

    // If worker config sets explicit endpoint override (e.g. for testing) use that
    if (overrideS3Endpoint != "") {
      s3Client.setEndpoint(overrideS3Endpoint);
    }

    if (s3PathStyle) {
      s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
    }

    s3 = new S3Writer(bucket, prefix, s3Client);

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
        BlockGZIPFileWriter buffer = tmpFiles.get(tp);
        if (buffer == null) {
          log.error("Trying to put {} records to partition {} which doesn't exist yet", records.size(), tp);
          throw new ConnectException("Trying to put records for a topic partition that has not be assigned");
        }
        buffer.write(record.value().toString());
      } catch (IOException e) {
        throw new RetriableException("Failed to write to buffer", e);
      }
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) throws ConnectException {
    // Don't rely on offsets passed. They have some quirks like including topic partitions that just
    // got revoked (i.e. we have deleted the writer already). Not sure if this is intended...
    // https://twitter.com/mr_paul_banks/status/702493772983177218

    // Instead iterate over the writers we do have and get the offsets directly from them.
    for (Map.Entry<TopicPartition, BlockGZIPFileWriter> entry : tmpFiles.entrySet()) {
      TopicPartition tp = entry.getKey();
      BlockGZIPFileWriter writer = entry.getValue();
      if (writer.getNumRecords() == 0) {
        // Not done anything yet
        log.info("No new records for partition {}", tp);
        continue;
      }
      try {
        writer.close();

        long nextOffset = s3.putChunk(writer.getDataFilePath(), writer.getIndexFilePath(), tp);

        // Now reset writer to a new one
        tmpFiles.put(tp, this.createNextBlockWriter(tp, nextOffset));
        log.info("Successfully uploaded chunk for {} now at offset {}", tp, nextOffset);
      } catch (FileNotFoundException fnf) {
        throw new ConnectException("Failed to find local dir for temp files", fnf);
      } catch (IOException e) {
        throw new RetriableException("Failed S3 upload", e);
      }
    }
  }

  private BlockGZIPFileWriter createNextBlockWriter(TopicPartition tp, long nextOffset) throws ConnectException, IOException {
    String name = String.format("%s-%05d", tp.topic(), tp.partition());
    return new BlockGZIPFileWriter(name, bufferDirectoryPath, nextOffset, maxBlockSize);
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) throws ConnectException {
    recoverAssignment(partitions);
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) throws ConnectException {
    for (TopicPartition tp : partitions) {
      // See if this is a new assignment
      BlockGZIPFileWriter writer = this.tmpFiles.remove(tp);
      if (writer != null) {
        log.info("Revoked partition {} deleting buffer", tp);
        try {
          writer.close();
          writer.delete();
        } catch (IOException ioe) {
          throw new ConnectException("Failed to resume TopicPartition form S3", ioe);
        }
      }
    }
  }

  private void recoverAssignment(Collection<TopicPartition> partitions) throws ConnectException {
    for (TopicPartition tp : partitions) {
      // See if this is a new assignment
      if (this.tmpFiles.get(tp) == null) {
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

    BlockGZIPFileWriter w = createNextBlockWriter(tp, offset);
    tmpFiles.put(tp, w);

    this.context.offset(tp, offset);
    this.context.resume(tp);
  }
}

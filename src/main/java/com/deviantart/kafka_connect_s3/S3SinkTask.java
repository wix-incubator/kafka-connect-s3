package com.deviantart.kafka_connect_s3;

import java.util.HashMap;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class S3SinkTask extends SinkTask {

  private Map<String, String> config;

  private Map<TopicPartition, BlockGZIPFileWriter> tmpFiles;

  long GZIPChunkThreshold = 67108864;

  public S3SinkTask() {
    tmpFiles = new HashMap<>();
  }

  @Override
  public String version() {
    return S3SinkConnectorConstants.VERSION;
  }

  @Override
  public void start(Map<String, String> props) {
    config = props;
    String chunkThreshold = config.get("compressed_block_size");
    if (chunkThreshold == null) {
      try {
        this.GZIPChunkThreshold = Long.parseLong(chunkThreshold);
      } catch (NumberFormatException nfe) {
        // keep default
      }
    }
  }

  @Override
  public void stop() throws ConnectException {
    // TODO should we just flush and close current buffer files
    // to resume when we start again, or should we just ignore and nuke them on startup
    // and resume from last block stored in S3?
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
    for (TopicPartition tp : offsets.keySet()) {
      BlockGZIPFileWriter writer = tmpFiles.get(tp);
      if (writer == null) {
        throw new ConnectException("Trying to flush records for a topic partition that has not be assigned");
      }
      try {
        writer.close();
        //this.uploadWriterFiles(writer);

        OffsetAndMetadata om = offsets.get(tp);

        // Also update the cursor file for partition so that we don't have expensive search to figure out
        // where we got to later.
        //this.updateCursorFile(tp, writer);*/

        // Now reset writer to a new one
        tmpFiles.put(tp, this.createNextBlockWriter(tp, om.offset()));
      } catch (FileNotFoundException fnf) {
        throw new ConnectException("Failed to find local dir for temp files", fnf);
      } catch (IOException e) {
        throw new RetriableException("Failed S3 upload", e);
      }
    }
  }

  private BlockGZIPFileWriter createNextBlockWriter(TopicPartition tp, long nextOffset) throws IOException {
    String name = String.format("%s-%05d", tp.topic(), tp.partition());
    String path = config.get("tmp_path");
    return new BlockGZIPFileWriter(name, path, nextOffset, this.GZIPChunkThreshold);
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    // TODO lookup in S3 what offset was for last good block and initialize buffer
    // and this.context.offsets() with the correct location.
    // This might be unsafe with a US Standard S3 region but ignore for now?
    // Perhaps need to wipe any partial local file?
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    // Stop updating and wipe local buffer file
    // Probably need to inform this.context.offsets() about the last offsets actually
    // committed...
  }
}

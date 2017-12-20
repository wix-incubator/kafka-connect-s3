package com.personali.kafka.connect.cloud.storage.storage;

import com.personali.kafka.connect.cloud.storage.partition.StoragePartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;


/**
 * StorageWriter provides necessary operations over a Cloud Storage to store files and retrieve
 * Last commit offsets for a TopicPartition.
 */
public abstract class StorageWriter {
  private static final Logger log = LoggerFactory.getLogger(StorageWriter.class);
  protected Map<String,String> config;
  protected String bucket;

  private long GZIPChunkThreshold = 67108864;

  public StorageWriter(Map<String,String> config) {
    this.config = config;
    String chunkThreshold = config.get("compressed_block_size");
    if (chunkThreshold != null) {
      try {
        this.GZIPChunkThreshold = Long.parseLong(chunkThreshold);
      } catch (NumberFormatException nfe) {
        // keep default
      }
    }
    if (config.containsKey("storage.bucket")){
      bucket = config.get("storage.bucket");
    }
    //For backward compatibility
    else{
      bucket = config.get("s3.bucket");
    }
    if (bucket == null || bucket == "") {
      throw new ConnectException("Cloud Storage bucket must be configured");
    }
  }

  private String getTopicKeyPrefix(String topic){
    String topicKeyPrefix=null;
    if (config.containsKey("storage.prefix."+topic)){
      topicKeyPrefix=config.get("storage.prefix."+topic);
    }
    //For backward compatability. Will be deprecated
    else if (config.containsKey("s3.prefix."+topic)){
      topicKeyPrefix=config.get("s3.prefix."+topic);
    }

    if (topicKeyPrefix != null){
      if (topicKeyPrefix.length() > 0 && !topicKeyPrefix.endsWith("/")) {
        topicKeyPrefix += "/";
      }
      return topicKeyPrefix;
    }
    else{
      return "";
    }
  }

  protected long getNextOffsetFromIndexFileContents(Reader indexJSON) throws IOException {
    try {
      JSONParser parser = new JSONParser();
      Object obj = parser.parse(indexJSON);
      JSONObject index = (JSONObject) obj;
      JSONArray chunks = (JSONArray) index.get("chunks");

      // Only need last chunk to figure out next offset
      Object lastChunkObj = chunks.get(chunks.size() - 1);
      JSONObject lastChunk = (JSONObject) lastChunkObj;
      long lastChunkOffset = (long) lastChunk.get("first_record_offset");
      long lastChunkLength = (long) lastChunk.get("num_records");
      return lastChunkOffset + lastChunkLength;
    } catch (ParseException pe) {
      throw new IOException("Failed for parse index file.", pe);
    }
  }

  // return the filename and storage full path using keyPrefix and StoragePartition logic
  protected String getChunkDataFileKey(String localDatafile, StoragePartition tpKey) {
    Path p = Paths.get(localDatafile);
    return String.format("%s%s%s", getTopicKeyPrefix(tpKey.getTopic()), tpKey.getDataDirectory(), p.getFileName().toString());
  }

  protected String getChunkIndexFileKey(String localIndexFile, StoragePartition tpKey) {
    Path p = Paths.get(localIndexFile);
    return String.format("%s%s%s", getTopicKeyPrefix(tpKey.getTopic()), tpKey.getIndexDirectory(), p.getFileName().toString());
  }

  protected String getTopicPartitionCursorFile(TopicPartition tp) {
    return String.format("%slast_chunk_index.%s-%05d.txt", getTopicKeyPrefix(tp.topic()), tp.topic(), tp.partition());
  }

  public abstract long fetchOffset(TopicPartition tp) throws IOException;

  public abstract String putChunk(String localDataFile, String localIndexFile,StoragePartition tpKey) throws IOException;

  public abstract void updateCursorFile(TopicPartition topicPartition, Long nextOffset) throws IOException;

  public long getGZIPChunkThreshold() {
    return GZIPChunkThreshold;
  }

  public void setGZIPChunkThreshold(long GZIPChunkThreshold) {
    this.GZIPChunkThreshold = GZIPChunkThreshold;
  }

}
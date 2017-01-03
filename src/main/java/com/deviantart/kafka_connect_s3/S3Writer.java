package com.deviantart.kafka_connect_s3;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.deviantart.kafka_connect_s3.partition.S3Partition;
import org.apache.kafka.common.TopicPartition;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;


/**
 * S3Writer provides necessary operations over S3 to store files and retrieve
 * Last commit offsets for a TopicPartition.
 *
 * Maybe one day we could make this an interface and support pluggable storage backends...
 * but for now it's just to keep things simpler to test.
 */
public class S3Writer {
  private static final Logger log = LoggerFactory.getLogger(S3Writer.class);
  private Map<String,String> config;
  private String bucket;
  private AmazonS3 s3Client;
  private TransferManager tm;

  public S3Writer(String bucket, Map<String,String> config) {
    this(bucket, config, new AmazonS3Client(new ProfileCredentialsProvider()));
  }

  public S3Writer(String bucket, Map<String,String> config, AmazonS3 s3Client) {
    this(bucket, config, s3Client, new TransferManager(s3Client));
  }

  public S3Writer(String bucket, Map<String,String> config, AmazonS3 s3Client, TransferManager tm) {
    this.config = config;
    this.bucket = bucket;
    this.s3Client = s3Client;
    this.tm = tm;
  }

  private String getTopicKeyPrefix(String topic){
    if (config.containsKey("s3.prefix."+topic)){
      String topicKeyPrefix = config.get("s3.prefix."+topic);
      if (topicKeyPrefix.length() > 0 && !topicKeyPrefix.endsWith("/")) {
        topicKeyPrefix += "/";
      }
      return topicKeyPrefix;
    }
    else{
      return "";
    }
  }

  public String putChunk(String localDataFile, String localIndexFile, S3Partition tpKey) throws IOException {
    // Put data file then index, then finally update/create the last_index_file marker
    String dataFileKey = this.getChunkDataFileKey(localDataFile,tpKey);
    String idxFileKey = this.getChunkIndexFileKey(localIndexFile,tpKey);
    // Read offset first since we'll delete the file after upload
    //long nextOffset = getNextOffsetFromIndexFileContents(new FileReader(localIndexFile));

    try {
      log.debug("Uploading file {} to {} {}",localDataFile,dataFileKey,bucket);
      Upload upload = tm.upload(this.bucket, dataFileKey, new File(localDataFile));
      upload.waitForCompletion();
      log.debug("Uploading file {} to {} {}",localIndexFile,idxFileKey,bucket);
      upload = tm.upload(this.bucket, idxFileKey, new File(localIndexFile));
      upload.waitForCompletion();
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException("Failed to upload to S3", e);
    }

    //Return data file S3 key
    return dataFileKey;
  }

  public long fetchOffset(TopicPartition tp) throws IOException {

    long nextOffset;

    // See if cursor file exists
    try (
      S3Object cursorObj = s3Client.getObject(this.bucket, this.getTopicPartitionCursorFile(tp));
      InputStreamReader input = new InputStreamReader(cursorObj.getObjectContent(), "UTF-8");
    ) {
      StringBuilder sb = new StringBuilder(1024);
      final char[] buffer = new char[1024];

      for(int read = input.read(buffer, 0, buffer.length);
              read != -1;
              read = input.read(buffer, 0, buffer.length)) {
        sb.append(buffer, 0, read);
      }
      nextOffset = Long.parseLong(sb.toString());
      return nextOffset;

    } catch (AmazonS3Exception ase) {
      if (ase.getStatusCode() == 404) {
        // Topic partition has no data in S3, start from beginning
        return 0;
      } else {
        throw new IOException("Failed to fetch cursor file", ase);
      }
    } catch (Exception e) {
      throw new IOException("Failed to fetch or read cursor file", e);
    }
  }

  private long getNextOffsetFromIndexFileContents(Reader indexJSON) throws IOException {
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

  // return the filename and S3 full path using keyPrefix and S3Partition logic
  private String getChunkDataFileKey(String localDatafile, S3Partition tpKey) {
    Path p = Paths.get(localDatafile);
    return String.format("%s%s%s", getTopicKeyPrefix(tpKey.getTopic()), tpKey.getDataDirectory(), p.getFileName().toString());
  }

  private String getChunkIndexFileKey(String localIndexFile, S3Partition tpKey) {
    Path p = Paths.get(localIndexFile);
    return String.format("%s%s%s", getTopicKeyPrefix(tpKey.getTopic()), tpKey.getIndexDirectory(), p.getFileName().toString());
  }

  private String getTopicPartitionCursorFile(TopicPartition tp) {
    return String.format("%slast_chunk_index.%s-%05d.txt", getTopicKeyPrefix(tp.topic()), tp.topic(), tp.partition());
  }

  public void updateCursorFile(TopicPartition topicPartition, Long nextOffset) throws IOException {
    try
    {
      byte[] contentAsBytes = nextOffset.toString().getBytes("UTF-8");
      ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(contentAsBytes);
      ObjectMetadata md = new ObjectMetadata();
      md.setContentLength(contentAsBytes.length);
      s3Client.putObject(new PutObjectRequest(this.bucket, getTopicPartitionCursorFile(topicPartition),
        contentsAsStream, md));
    }
    catch(Exception ex)
    {
      throw new IOException("Failed to update cursor file", ex);
    }
  }
}
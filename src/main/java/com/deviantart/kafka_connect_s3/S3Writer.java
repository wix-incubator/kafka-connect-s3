package com.deviantart.kafka_connect_s3;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.kafka.common.TopicPartition;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;


/**
 * S3Writer provides necessary operations over S3 to store files and retrieve
 * Last commit offsets for a TopicPartition.
 *
 * Maybe one day we could make this an interface and support pluggable storage backends...
 * but for now it's just to keep things simpler to test.
 */
public class S3Writer {
  private String keyPrefix;
  private String bucket;
  private AmazonS3 s3Client;
  private TransferManager tm;
  private Map<String, String> config;

  public S3Writer(String bucket, String keyPrefix, Map<String, String> config) {
    this(bucket, keyPrefix, new AmazonS3Client(new ProfileCredentialsProvider()), config);
  }

  public S3Writer(String bucket, String keyPrefix, AmazonS3 s3Client, Map<String, String> config) {
    this(bucket, keyPrefix, s3Client, new TransferManager(s3Client), config);
  }

  public S3Writer(String bucket, String keyPrefix, AmazonS3 s3Client, TransferManager tm, Map<String, String> config) {
    if (keyPrefix.length() > 0 && !keyPrefix.endsWith("/")) {
      keyPrefix += "/";
    }
    this.keyPrefix = keyPrefix;
    this.bucket = bucket;
    this.s3Client = s3Client;
    this.tm = tm;
    this.config = config;
  }

  public long putChunk(String localDataFile, String localIndexFile, TopicPartition tp) throws IOException {
    // Put data file then index, then finally update/create the last_index_file marker
    String dataFileKey = this.getChunkFileKey(localDataFile);
    String idxFileKey = this.getChunkFileKey(localIndexFile);
    // Read offset first since we'll delete the file after upload
    long nextOffset = getNextOffsetFromIndexFileContents(new FileReader(localIndexFile));

    try {
      PutObjectRequest dataRequest = this.configurePutObjectRequest(
              new PutObjectRequest(this.bucket, dataFileKey, new File(localDataFile)), config);
      Upload upload = tm.upload(dataRequest);
      upload.waitForCompletion();
      PutObjectRequest keyRequest = this.configurePutObjectRequest(
              new PutObjectRequest(this.bucket, idxFileKey, new File(localIndexFile)), config);
      upload = tm.upload(keyRequest);
      upload.waitForCompletion();
    } catch (Exception e) {
      throw new IOException("Failed to upload to S3", e);
    }

    this.updateCursorFile(idxFileKey, tp);

    // Sanity check - return what the new nextOffset will be based on the index we just uploaded
    return nextOffset;
  }

  public long fetchOffset(TopicPartition tp) throws IOException {

    // See if cursor file exists
    String indexFileKey;

    try (
      S3Object cursorObj = s3Client.getObject(this.bucket, this.getTopicPartitionLastIndexFileKey(tp));
      InputStreamReader input = new InputStreamReader(cursorObj.getObjectContent(), "UTF-8");
    ) {
      StringBuilder sb = new StringBuilder(1024);
      final char[] buffer = new char[1024];

      for(int read = input.read(buffer, 0, buffer.length);
              read != -1;
              read = input.read(buffer, 0, buffer.length)) {
        sb.append(buffer, 0, read);
      }
      indexFileKey = sb.toString();
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

    // Now fetch last written index file...
    try (
      S3Object indexObj = s3Client.getObject(this.bucket, indexFileKey);
      InputStreamReader isr = new InputStreamReader(indexObj.getObjectContent(), "UTF-8");
    ) {
      return getNextOffsetFromIndexFileContents(isr);
    } catch (Exception e) {
      throw new IOException("Failed to fetch or parse last index file", e);
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

  // We store chunk files with a date prefix just to make finding them and navigating around the bucket a bit easier
  // date is meaningless other than "when this was uploaded"
  private String getChunkFileKey(String localFilePath) {
    Path p = Paths.get(localFilePath);
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    return String.format("%s%s/%s", keyPrefix, df.format(new Date()), p.getFileName().toString());
  }

  private String getTopicPartitionLastIndexFileKey(TopicPartition tp) {
    return String.format("%slast_chunk_index.%s-%05d.txt", this.keyPrefix, tp.topic(), tp.partition());
  }

  private void updateCursorFile(String lastIndexFileKey, TopicPartition tp) throws IOException {
    try
    {
      byte[] contentAsBytes = lastIndexFileKey.getBytes("UTF-8");
      ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(contentAsBytes);
      ObjectMetadata md = new ObjectMetadata();
      md.setContentLength(contentAsBytes.length);
      PutObjectRequest request = this.configurePutObjectRequest(
              new PutObjectRequest(this.bucket, this.getTopicPartitionLastIndexFileKey(tp), contentsAsStream, md),
              this.config);
      s3Client.putObject(request);
    }
    catch(Exception ex)
    {
      throw new IOException("Failed to update cursor file", ex);
    }
  }

  private PutObjectRequest configurePutObjectRequest(PutObjectRequest request, Map<String, String> config)
          throws IOException {
    PutObjectRequest req = request.clone();
    try {
      Boolean s3EnableSse = Boolean.parseBoolean(config.get("s3.enableSse"));
      if (s3EnableSse) {
        String s3KmsKeyId = config.get("s3.kmsKeyId");
        req.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(s3KmsKeyId));
      }
      return req;
    } catch(Exception ex) {
      throw new IOException("Failed to create PUT object request.", ex);
    }
  }
}
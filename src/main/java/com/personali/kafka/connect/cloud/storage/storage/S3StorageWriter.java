package com.personali.kafka.connect.cloud.storage.storage;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.personali.kafka.connect.cloud.storage.partition.StoragePartition;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;


/**
 * StorageWriter provides necessary operations over S3 to store files and retrieve
 * Last commit offsets for a TopicPartition.
 *
 * Maybe one day we could make this an interface and support pluggable storage backends...
 * but for now it's just to keep things simpler to test.
 */
public class S3StorageWriter extends StorageWriter {
  private static final Logger log = LoggerFactory.getLogger(S3StorageWriter.class);
  private AmazonS3 s3Client;
  private TransferManager tm;

  //Constructor for tests
  public S3StorageWriter(Map<String,String> config, AmazonS3 s3Client, TransferManager transferManager){
    super(config);

    // If worker config sets explicit endpoint override (e.g. for testing) use that
    String s3Endpoint = config.get("s3.endpoint");
    if (s3Endpoint != null && s3Endpoint != "") {
      s3Client.setEndpoint(s3Endpoint);
    }
    Boolean s3PathStyle = Boolean.parseBoolean(config.get("s3.path_style"));
    if (s3PathStyle) {
      s3Client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build());
    }

    this.s3Client = s3Client;
    this.tm = transferManager;
  }

  public S3StorageWriter(Map<String,String> config) {
    super(config);

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

    this.s3Client = s3Client;
    this.tm = new TransferManager(s3Client);
  }

  @Override
  public String putChunk(String localDataFile, String localIndexFile,StoragePartition tpKey) throws IOException {
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

  @Override
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
      nextOffset = Long.parseLong(sb.toString().trim().replace("\n",""));
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

  @Override
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
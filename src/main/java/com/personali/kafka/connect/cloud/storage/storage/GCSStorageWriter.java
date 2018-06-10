package com.personali.kafka.connect.cloud.storage.storage;

import com.personali.kafka.connect.cloud.storage.partition.StoragePartition;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;


/**
 * StorageWriter provides necessary operations over GCS to store files and retrieve
 * Last commit offsets for a TopicPartition.
 */
public class GCSStorageWriter extends StorageWriter {
  private static final Logger log = LoggerFactory.getLogger(GCSStorageWriter.class);
  private Storage storage;

  public GCSStorageWriter(Map<String,String> config) {
    super(config);
    // Instantiates a client
    this.storage = StorageOptions.getDefaultInstance().getService();
  }

  @Override
  public String putChunk(String localDataFile, String localIndexFile,StoragePartition tpKey) throws IOException {
    // Put data file then index, then finally update/create the last_index_file marker
    String dataFileKey = this.getChunkDataFileKey(localDataFile,tpKey);
    String idxFileKey = this.getChunkIndexFileKey(localIndexFile,tpKey);
    // Read offset first since we'll delete the file after upload
    //long nextOffset = getNextOffsetFromIndexFileContents(new FileReader(localIndexFile));

    //Retry uploading files 3 times with 5 second backoff between each retry before failing
    retryUpload(1,3,5,localDataFile,dataFileKey,localIndexFile,idxFileKey);

    //Return data file GCS key
    return dataFileKey;
  }

  private void retryUpload(int time, int limit, int backoffSeconds, String localDataFile, String dataFileKey,
                           String localIndexFile, String idxFileKey) throws IOException {
    try {
      log.debug("Uploading file {} to {} {}",localDataFile,dataFileKey,bucket);

      uploadLocalFile(new File(localDataFile).toPath(),
              BlobInfo.newBuilder(this.bucket, dataFileKey).build());

      log.debug("Uploading file {} to {} {}",localIndexFile,idxFileKey,bucket);

      uploadLocalFile(new File(localIndexFile).toPath(),
              BlobInfo.newBuilder(this.bucket, idxFileKey).build());

    } catch (Exception e) {
      log.error("Failed uploading file.");
      if (time <= limit){
        log.error("Retrying in {} seconds", backoffSeconds);
        try {
          Thread.sleep(backoffSeconds*1000);
        } catch (InterruptedException e1) {
        }
        retryUpload(time+1, limit, backoffSeconds,localDataFile,dataFileKey,localIndexFile,idxFileKey);
      }
      else {
        log.error("Finished all retries. Failing.");
        throw new IOException("Failed to upload to GCS", e);
      }
    }

  }


  @Override
  public long fetchOffset(TopicPartition tp) throws IOException {

    long nextOffset;

    try {
      Blob blob = storage.get(this.bucket, this.getTopicPartitionCursorFile(tp));
      // See if cursor file exists
      if (blob != null) {
        String strContent = new String(blob.getContent(), "UTF-8");
        nextOffset = Long.parseLong(strContent.trim().replace("\n", ""));
        return nextOffset;
      }
      else{
        return 0;
      }

    } catch (Exception e) {
      throw new IOException("Failed to fetch or read cursor file", e);
    }
  }

  @Override
  public void updateCursorFile(TopicPartition topicPartition, Long nextOffset) throws IOException {
    try
    {
      uploadSmallContent(nextOffset.toString(),
              BlobInfo.newBuilder(this.bucket, getTopicPartitionCursorFile(topicPartition)).build());
    }
    catch(Exception ex)
    {
      throw new IOException("Failed to update cursor file", ex);
    }
  }

  private void uploadLocalFile(Path uploadFrom, BlobInfo blobInfo) throws IOException {
    if (Files.size(uploadFrom) > 1_000_000) {
      // When content is not available or large (1MB or more) it is recommended
      // to write it in chunks via the blob's channel writer.
      try (WriteChannel writer = storage.writer(blobInfo)) {
        byte[] buffer = new byte[1024];
        try (InputStream input = Files.newInputStream(uploadFrom)) {
          int limit;
          while ((limit = input.read(buffer)) >= 0) {
            try {
              writer.write(ByteBuffer.wrap(buffer, 0, limit));
            } catch (Exception ex) {
              ex.printStackTrace();
              throw ex;
            }
          }
        }
      }
    } else {
      byte[] bytes = Files.readAllBytes(uploadFrom);
      // create the blob in one request.
      storage.create(blobInfo, bytes);
    }
    log.info("Blob was created");
  }

  private void uploadSmallContent(String content, BlobInfo blobInfo) throws IOException {
      byte[] bytes = content.getBytes("UTF-8");
      // create the blob in one request.
      storage.create(blobInfo, bytes);
      log.info("Blob was created");
  }
}
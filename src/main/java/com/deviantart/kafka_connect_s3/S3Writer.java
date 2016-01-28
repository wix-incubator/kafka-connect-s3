package com.deviantart.kafka_connect_s3;

import java.io.BufferedReader;
import java.lang.StringBuilder;
import java.io.InputStreamReader;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.UploadPartRequest;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;


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

  public S3Writer(String bucket, String keyPrefix) {
    this(bucket, keyPrefix, new AmazonS3Client(new ProfileCredentialsProvider()));
  }

  public S3Writer(String bucket, String keyPrefix, AmazonS3 s3Client) {
    this.keyPrefix = keyPrefix;
    this.bucket = bucket;
    this.s3Client = s3Client;
  }

  public void putChunk(String localDataFile, String localIndexFile, TopicPartition tp) throws IOException {
    // Put data file then index, then finally update/create the last_index_file marker
    String dataFileKey = this.getChunkFileKey(localDataFile);
    String idxFileKey = this.getChunkFileKey(localIndexFile);
    this.uploadFile(dataFileKey, localDataFile);
    this.uploadFile(idxFileKey, localIndexFile);
    this.updateCursorFile(idxFileKey, tp);
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
      JSONParser parser = new JSONParser();

      Object obj = parser.parse(isr);
      JSONObject index = (JSONObject) obj;
      JSONArray chunks = (JSONArray) index.get("chunks");

      // Only need last chunk to figure out next offset
      Object lastChunkObj = chunks.get(chunks.size() - 1);
      JSONObject lastChunk = (JSONObject) lastChunkObj;
      long lastChunkOffset = (long) lastChunk.get("first_record_offset");
      long lastChunkLength = (long) lastChunk.get("num_records");
      return lastChunkOffset + lastChunkLength;

    } catch (Exception e) {
      throw new IOException("Failed to fetch or parse last index file", e);
    }
  }

  private void uploadFile(String name, String localPath) throws IOException {
    // Create a list of UploadPartResponse objects. You get one of these
    // for each part upload.
    List<PartETag> partETags = new ArrayList<PartETag>();

    // Step 1: Initialize.
    InitiateMultipartUploadRequest initRequest = new
         InitiateMultipartUploadRequest(this.bucket, name);
    InitiateMultipartUploadResult initResponse =
                         s3Client.initiateMultipartUpload(initRequest);

    File file = new File(localPath);
    long contentLength = file.length();
    long partSize = 5 * 1024 * 1024; // Set part size to 5 MB.

    try {
      // Step 2: Upload parts.
      long filePosition = 0;
      for (int i = 1; filePosition < contentLength; i++) {
          // Last part can be less than 5 MB. Adjust part size.
        partSize = Math.min(partSize, (contentLength - filePosition));

        // Create request to upload a part.
        UploadPartRequest uploadRequest = new UploadPartRequest()
          .withBucketName(this.bucket).withKey(name)
          .withUploadId(initResponse.getUploadId()).withPartNumber(i)
          .withFileOffset(filePosition)
          .withFile(file)
          .withPartSize(partSize);

        // Upload part and add response to our list.
        partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());

        filePosition += partSize;
      }

      // Step 3: Complete.
      CompleteMultipartUploadRequest compRequest = new
                  CompleteMultipartUploadRequest(this.bucket,
                                                 name,
                                                 initResponse.getUploadId(),
                                                 partETags);

      s3Client.completeMultipartUpload(compRequest);

      // All done, delete the local copy
      file.delete();

    } catch (Exception e) {
        s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(
                  this.bucket, name, initResponse.getUploadId()));
        throw new IOException("Failed to complete Multipart Upload", e);
    }
  }

  // We store chunk files with a date prefix just to make finding them and navigating around the bucket a bit easier
  // date is meaningless other than "when this was uploaded"
  private String getChunkFileKey(String localFilePath) {
    Path p = Paths.get(localFilePath);
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    return String.format("%s/%s/%s", keyPrefix, df.format(new Date()), p.getFileName().toString());
  }

  private String getTopicPartitionLastIndexFileKey(TopicPartition tp) {
    return String.format("%s/last_chunk_index.%s-%05d.txt", this.keyPrefix, tp.topic(), tp.partition());
  }

  private void updateCursorFile(String lastIndexFileKey, TopicPartition tp) throws IOException {
    try
    {
      byte[] contentAsBytes = lastIndexFileKey.getBytes("UTF-8");
      ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(contentAsBytes);
      ObjectMetadata md = new ObjectMetadata();
      md.setContentLength(contentAsBytes.length);
      s3Client.putObject(new PutObjectRequest(this.bucket, this.getTopicPartitionLastIndexFileKey(tp),
        contentsAsStream, md));
    }
    catch(Exception ex)
    {
      throw new IOException("Failed to update cursor file", ex);
    }
  }
}
package com.deviantart.kafka_connect_s3;

import static org.mockito.Mockito.*;
import org.mockito.ArgumentCaptor;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

import org.apache.kafka.common.TopicPartition;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.lang.StringBuilder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Really basic sanity check testing over the documented use of API.
 * I've not bothered to properly mock result objects etc. as it really only tests
 * how well I can mock S3 API beyond a certain point.
 */
public class S3WriterTest extends TestCase {

  private String testBucket = "kafka-connect-s3-unit-test";
  private String tmpDirPrefix = "S3WriterTest";
  private String tmpDir;

  public S3WriterTest(String testName) {
    super(testName);

    String tempDir = System.getProperty("java.io.tmpdir");
    this.tmpDir = tempDir.concat(tmpDirPrefix);

    System.out.println("Temp dir for writer test is: " + tmpDir);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(S3WriterTest.class);
  }

  @Override
  protected void setUp() throws Exception {
    File f = new File(tmpDir);

    if (!f.exists()) {
      f.mkdir();
    }
  }

  private BlockGZIPFileWriter createDummmyFiles(long offset, int numRecords) throws Exception {
    BlockGZIPFileWriter writer = new BlockGZIPFileWriter("bar-00000", tmpDir, offset);
    for (int i = 0; i < numRecords; i++) {
      writer.write(String.format("Record %d", i));
    }
    writer.close();
    return writer;
  }

  class ExpectedRequestParams {
    public String key;
    public String bucket;
    public ExpectedRequestParams(String k, String b) {
      key = k;
      bucket = b;
    }
  }

  private void verifyTMUpload(TransferManager mock, ExpectedRequestParams[] expect) {
    ArgumentCaptor<String> bucketCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    verify(mock, times(expect.length)).upload(bucketCaptor.capture(), keyCaptor.capture()
      ,any(File.class));

    List<String> bucketArgs = bucketCaptor.getAllValues();
    List<String> keyArgs = keyCaptor.getAllValues();

    for (int i = 0 ; i < expect.length; i++) {
      assertEquals(expect[i].bucket, bucketArgs.remove(0));
      assertEquals(expect[i].key, keyArgs.remove(0));
    }
  }

  private void verifyStringPut(AmazonS3 mock, String key, String content) throws Exception {
    ArgumentCaptor<PutObjectRequest> argument
      = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(mock)
      .putObject(argument.capture());

    PutObjectRequest req = argument.getValue();
    assertEquals(key, req.getKey());
    assertEquals(this.testBucket, req.getBucketName());

    InputStreamReader input = new InputStreamReader(req.getInputStream(), "UTF-8");
    StringBuilder sb = new StringBuilder(1024);
    final char[] buffer = new char[1024];
    try {
      for(int read = input.read(buffer, 0, buffer.length);
              read != -1;
              read = input.read(buffer, 0, buffer.length)) {
        sb.append(buffer, 0, read);
      }
    } catch (IOException ignore) { }

    assertEquals(content, sb.toString());
  }

  private String getKeyForFilename(String prefix, String name) {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    return String.format("%s/%s/%s", prefix, df.format(new Date()), name);
  }

  public void testUpload() throws Exception {
    AmazonS3 s3Mock = mock(AmazonS3.class);
    TransferManager tmMock = mock(TransferManager.class);
    BlockGZIPFileWriter fileWriter = createDummmyFiles(0, 1000);
    S3Writer s3Writer = new S3Writer(testBucket, "pfx", s3Mock, tmMock);
    TopicPartition tp = new TopicPartition("bar", 0);

    Upload mockUpload = mock(Upload.class);

    when(tmMock.upload(eq(testBucket), eq(getKeyForFilename("pfx", "bar-00000-000000000000.gz")), isA(File.class)))
      .thenReturn(mockUpload);
    when(tmMock.upload(eq(testBucket), eq(getKeyForFilename("pfx", "bar-00000-000000000000.index.json")), isA(File.class)))
      .thenReturn(mockUpload);

    s3Writer.putChunk(fileWriter.getDataFilePath(), fileWriter.getIndexFilePath(), tp);

    verifyTMUpload(tmMock, new ExpectedRequestParams[]{
      new ExpectedRequestParams(getKeyForFilename("pfx", "bar-00000-000000000000.gz"), testBucket),
      new ExpectedRequestParams(getKeyForFilename("pfx", "bar-00000-000000000000.index.json"), testBucket)
    });

    // Verify it also wrote the index file key
    verifyStringPut(s3Mock, "pfx/last_chunk_index.bar-00000.txt",
      getKeyForFilename("pfx", "bar-00000-000000000000.index.json"));
  }

  private S3Object makeMockS3Object(String key, String contents) throws Exception {
    S3Object mock = new S3Object();
    mock.setBucketName(this.testBucket);
    mock.setKey(key);
    InputStream stream = new ByteArrayInputStream(contents.getBytes("UTF-8"));
    mock.setObjectContent(stream);
    System.out.println("MADE MOCK FOR "+key+" WITH BODY: "+contents);
    return mock;
  }

  public void testFetchOffsetNewTopic() throws Exception {
    AmazonS3 s3Mock = mock(AmazonS3.class);
    S3Writer s3Writer = new S3Writer(testBucket, "pfx", s3Mock);

    // Non existing topic should return 0 offset
    // Since the file won't exist. code will expect the initial fetch to 404
    AmazonS3Exception ase = new AmazonS3Exception("The specified key does not exist.");
    ase.setStatusCode(404);
    when(s3Mock.getObject(eq(testBucket), eq("pfx/last_chunk_index.new_topic-00000.txt")))
      .thenThrow(ase)
      .thenReturn(null);

    TopicPartition tp = new TopicPartition("new_topic", 0);
    long offset = s3Writer.fetchOffset(tp);
    assertEquals(0, offset);
    verify(s3Mock).getObject(eq(testBucket), eq("pfx/last_chunk_index.new_topic-00000.txt"));
  }

  public void testFetchOffsetExistingTopic() throws Exception {
    AmazonS3 s3Mock = mock(AmazonS3.class);
    S3Writer s3Writer = new S3Writer(testBucket, "pfx", s3Mock);
    // Existing topic should return correct offset
    // We expect 2 fetches, one for the cursor file
    // and second for the index file itself
    String indexKey = getKeyForFilename("pfx", "bar-00000-000000010042.index.json");

    when(s3Mock.getObject(eq(testBucket), eq("pfx/last_chunk_index.bar-00000.txt")))
      .thenReturn(
        makeMockS3Object("pfx/last_chunk_index.bar-00000.txt", indexKey)
      );

    when(s3Mock.getObject(eq(testBucket), eq(indexKey)))
      .thenReturn(
        makeMockS3Object(indexKey,
          "{\"chunks\":["
          // Assume 10 byte records, split into 3 chunks for same of checking the logic about next offset
          // We expect next offset to be 12031 + 34
          +"{\"first_record_offset\":10042,\"num_records\":1000,\"byte_offset\":0,\"byte_length\":10000},"
          +"{\"first_record_offset\":11042,\"num_records\":989,\"byte_offset\":10000,\"byte_length\":9890},"
          +"{\"first_record_offset\":12031,\"num_records\":34,\"byte_offset\":19890,\"byte_length\":340}"
          +"]}"
        )
      );

    TopicPartition tp = new TopicPartition("bar", 0);
    long offset = s3Writer.fetchOffset(tp);
    assertEquals(12031+34, offset);
    verify(s3Mock).getObject(eq(testBucket), eq("pfx/last_chunk_index.bar-00000.txt"));
    verify(s3Mock).getObject(eq(testBucket), eq(indexKey));

  }
}

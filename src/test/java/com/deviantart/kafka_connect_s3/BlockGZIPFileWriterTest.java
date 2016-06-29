package com.deviantart.kafka_connect_s3;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class BlockGZIPFileWriterTest extends TestCase {

  private String tmpDirPrefix = "BlockGZIPFileWriterTest";
  private String tmpDir;

  public BlockGZIPFileWriterTest(String testName) {
    super(testName);

    String tempDir = System.getProperty("java.io.tmpdir");
    this.tmpDir = new File(tempDir, tmpDirPrefix).toString();

    System.out.println("Temp dir for writer test is: " + tmpDir);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(BlockGZIPFileWriterTest.class);
  }

  @Override
  protected void setUp() throws Exception {
    File f = new File(tmpDir);

    if (!f.exists()) {
      f.mkdir();
    }
  }

  public void testPaths() throws Exception {
    BlockGZIPFileWriter w = new BlockGZIPFileWriter("foo", tmpDir);
    assertEquals(tmpDir + "/foo-000000000000.gz", w.getDataFilePath());
    assertEquals(tmpDir + "/foo-000000000000.index.json", w.getIndexFilePath());


    BlockGZIPFileWriter w2 = new BlockGZIPFileWriter("foo", tmpDir, 123456);
    assertEquals(tmpDir + "/foo-000000123456.gz", w2.getDataFilePath());
    assertEquals(tmpDir + "/foo-000000123456.index.json", w2.getIndexFilePath());
  }

  public void testWrite() throws Exception {
    // Very compressible 200 byte padding string to prepend to our unique line prefix
    String pad = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
    + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    // Make a writer with artificially small chunk threshold of 1kb
    BlockGZIPFileWriter w = new BlockGZIPFileWriter("write-test", tmpDir, 987654321, 1000);

    int totalUncompressedBytes = 0;
    String[] expectedLines = new String[50];
    // 50 records * 200 bytes padding should be at least 10 chunks worth
    for (int i = 0; i < 50; i++) {
      String line = String.format("Record %d %s", i, pad);
      // Plus one for newline
      totalUncompressedBytes += line.length() + 1;
      // Expect to read without newlines...
      expectedLines[i] = line;
      // But add newlines to half the input to verify writer adds them only if needed
      if (i % 2 == 0) {
        line += "\n";
      }
      w.write(toRecord(line));
    }

    assertEquals(totalUncompressedBytes, w.getTotalUncompressedSize());
    assertEquals(50, w.getNumRecords());
    assertTrue("Should be at least 10 chunks in output file", w.getNumChunks() >= 10);

    w.close();

    verifyOutputIsSaneGZIPFile(w.getDataFilePath(), expectedLines);
    verifyIndexFile(w, 987654321, expectedLines);
  }

  static SinkRecord toRecord(String line) {
    return new SinkRecord(
            "topicIgnored",
            0,
            null,
            null,
            Schema.STRING_SCHEMA,
            line,
            123 // not important
    );
  }

  private void verifyOutputIsSaneGZIPFile(String filename, String[] expectedRecords) throws Exception {
    GZIPInputStream zip = new GZIPInputStream(new FileInputStream(filename));
    BufferedReader r = new BufferedReader(new InputStreamReader(zip, "UTF-8"));

    String line;
    int i = 0;
    while ((line = r.readLine()) != null) {
      assertTrue( String.format("Output file has more lines than expected. Expected %d lines", expectedRecords.length)
        , i < expectedRecords.length);

      String expectedLine = expectedRecords[i];
      assertEquals(String.format("Output file doesn't match, first difference on line %d", i), expectedLine, line);
      i++;
    }
  }

  private void verifyIndexFile(BlockGZIPFileWriter w, int startOffset, String[] expectedRecords) throws Exception {
    JSONParser parser = new JSONParser();

    Object obj = parser.parse(new FileReader(w.getIndexFilePath()));
    JSONObject index = (JSONObject) obj;
    JSONArray chunks = (JSONArray) index.get("chunks");

    assertEquals(w.getNumChunks(), chunks.size());

    RandomAccessFile file = new RandomAccessFile(w.getDataFilePath(), "r");

      // Check we can read all the chunks as individual gzip segments
    int expectedStartOffset = startOffset;
    int recordIndex = 0;
    int totalBytes = 0;
    int chunkIndex = 0;
    for (Object chunk : chunks) {
      JSONObject chunkObj = (JSONObject) chunk;
      int firstOffset = (int)(long) chunkObj.get("first_record_offset");
      int numRecords  = (int)(long) chunkObj.get("num_records");
      int byteOffset  = (int)(long) chunkObj.get("byte_offset");
      int byteLength  = (int)(long) chunkObj.get("byte_length");

      assertEquals(expectedStartOffset, firstOffset);
      assertTrue(byteLength > 0);
      assertTrue(byteOffset >= 0);

          // Read just that segment of the file into byte array and attempt to parse GZIP
      byte[] buffer = new byte[byteLength];
      file.seek(byteOffset);
      int numBytesRead = file.read(buffer);

      assertEquals(buffer.length, numBytesRead);

      GZIPInputStream zip = new GZIPInputStream(new ByteArrayInputStream(buffer));
      BufferedReader r = new BufferedReader(new InputStreamReader(zip, "UTF-8"));

      int numRecordsActuallyInChunk = 0;
      String line;
      while ((line = r.readLine()) != null) {
        assertEquals(expectedRecords[recordIndex], line);
        recordIndex++;
        numRecordsActuallyInChunk++;
      }

      assertEquals(numRecordsActuallyInChunk, numRecords);

      totalBytes += byteLength;

      expectedStartOffset = firstOffset + numRecords;

      chunkIndex++;
    }

    assertEquals("All chunks should cover all bytes in the file", totalBytes, file.length());
  }

  // Hmm this test is actually not very conclusive - on OS X and most linux file systems
  // it passes anyway due to nature of filesystems. Not sure how to write something more robust
  // though to validate that we definitiely truncate the files even if we write less data
  public void testShouldOverwrite() throws Exception {
    // Make writer and write to it a bit.
    {
      BlockGZIPFileWriter w = new BlockGZIPFileWriter("overwrite-test", tmpDir);

      // Write at least a few 4k blocks to disk so we can be sure that we don't
      // only overwrite the first block.
      String[] expectedLines = new String[5000];
      for (int i = 0; i < 5000; i++) {
        String line = String.format("Record %d", i);
        w.write(toRecord(line));
        expectedLines[i] = line;
      }

      assertEquals(5000, w.getNumRecords());

      w.close();

      // Just check it actually write to disk
      verifyOutputIsSaneGZIPFile(w.getDataFilePath(), expectedLines);
      verifyIndexFile(w, 0, expectedLines);

    }

    {
      // Now make a whole new writer for same chunk
      BlockGZIPFileWriter w = new BlockGZIPFileWriter("overwrite-test", tmpDir);

      // Only write a few lines
      String[] expectedLines2 = new String[10];
      for (int i = 0; i < 10; i++) {
        String line = String.format("Overwrite record %d", i);
        w.write(toRecord(line));
        expectedLines2[i] = line;
      }

      assertEquals(10, w.getNumRecords());

      w.close();

      // No check output is only the 10 lines we just wrote
      verifyOutputIsSaneGZIPFile(w.getDataFilePath(), expectedLines2);
      verifyIndexFile(w, 0, expectedLines2);
    }
  }

  public void testDelete() throws Exception {
    // Make writer and write to it a bit.
    BlockGZIPFileWriter w = new BlockGZIPFileWriter("overwrite-test", tmpDir);

    String[] expectedLines = new String[5000];
    for (int i = 0; i < 5000; i++) {
      String line = String.format("Record %d", i);
      w.write(toRecord(line));
      expectedLines[i] = line;
    }

    assertEquals(5000, w.getNumRecords());

    w.close();

    // Just check it actually write to disk
    verifyOutputIsSaneGZIPFile(w.getDataFilePath(), expectedLines);
    verifyIndexFile(w, 0, expectedLines);

    // Now remove it
    w.delete();

    File dataF = new File(w.getDataFilePath());
    File idxF = new File(w.getIndexFilePath());

    assertFalse("Data file should not exist after delete", dataF.exists());
    assertFalse("Index file should not exist after delete", idxF.exists());
  }
}

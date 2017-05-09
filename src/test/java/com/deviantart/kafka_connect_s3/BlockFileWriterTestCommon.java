package com.deviantart.kafka_connect_s3;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public abstract class BlockFileWriterTestCommon {

    protected static String tmpDir;

    @Before
    public void setUp() throws Exception {
        File f = new File(tmpDir);

        if (!f.exists()) {
            f.mkdir();
        }
    }

    abstract protected BlockFileWriter newBlockFileWriter(String filenameBase, String path) throws IOException;
    abstract protected BlockFileWriter newBlockFileWriter(String filenameBase, String path, long firstRecordOffset) throws IOException;
    abstract protected BlockFileWriter newBlockFileWriter(String filenameBase, String path, long firstRecordOffset, long chunkThreshold) throws IOException;
    abstract protected InputStream newCompressorInputStream(InputStream in) throws IOException;

    protected void verifyOutputIsSaneCompressedFile(String filename, String[] expectedRecords) throws Exception {
        InputStream zip = newCompressorInputStream(new FileInputStream(filename));
        BufferedReader r = new BufferedReader(new InputStreamReader(zip, "UTF-8"));

        String line;
        int i = 0;
        while ((line = r.readLine()) != null) {
            assertTrue(String.format("Output file has more lines than expected. Expected %d lines", expectedRecords.length)
                    , i < expectedRecords.length);

            String expectedLine = expectedRecords[i];
            assertEquals(String.format("Output file doesn't match, first difference on line %d", i), expectedLine, line);
            i++;
        }
    }

    protected void verifyIndexFile(BlockFileWriter w, int startOffset, String[] expectedRecords) throws Exception {
        JSONParser parser = new JSONParser();

        Object obj = parser.parse(new FileReader(w.getIndexFilePath()));
        JSONObject index = (JSONObject) obj;
        JSONArray chunks = (JSONArray) index.get("chunks");

        assertEquals(w.getNumChunks(), chunks.size());

        RandomAccessFile file = new RandomAccessFile(w.getDataFilePath(), "r");

        // Check we can read all the chunks as individual bzip2 segments
        int expectedStartOffset = startOffset;
        int recordIndex = 0;
        int totalBytes = 0;
        int chunkIndex = 0;
        for (Object chunk : chunks) {
            JSONObject chunkObj = (JSONObject) chunk;
            int firstOffset = (int) (long) chunkObj.get("first_record_offset");
            int numRecords = (int) (long) chunkObj.get("num_records");
            int byteOffset = (int) (long) chunkObj.get("byte_offset");
            int byteLength = (int) (long) chunkObj.get("byte_length");

            assertEquals(expectedStartOffset, firstOffset);
            assertTrue(byteLength > 0);
            assertTrue(byteOffset >= 0);

            // Read just that segment of the file into byte array and attempt to parse BZIP2
            byte[] buffer = new byte[byteLength];
            file.seek(byteOffset);
            int numBytesRead = file.read(buffer);

            assertEquals(buffer.length, numBytesRead);

            InputStream zip = newCompressorInputStream(new ByteArrayInputStream(buffer));
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
}

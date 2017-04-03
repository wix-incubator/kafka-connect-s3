package com.deviantart.kafka_connect_s3;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BlockBZIP2FileWriterTest extends BlockFileWriterTestCommon {

    private static final String tmpDirPrefix = "BlockBZIP2FileWriterTest";

    @BeforeClass
    public static void oneTimeSetUp() {

        String tempDir = System.getProperty("java.io.tmpdir");
        tmpDir = new File(tempDir, tmpDirPrefix).toString();

        System.out.println("Temp dir for writer test is: " + tmpDir);
    }

    @Override
    protected BlockFileWriter newBlockFileWriter(String filenameBase, String path) throws IOException {
        return new BlockBZIP2FileWriter(filenameBase, path);
    }

    @Override
    protected BlockFileWriter newBlockFileWriter(String filenameBase, String path, long firstRecordOffset) throws IOException {
        return new BlockBZIP2FileWriter(filenameBase, path, firstRecordOffset);
    }

    @Override
    protected BlockFileWriter newBlockFileWriter(String filenameBase, String path, long firstRecordOffset, long chunkThreshold) throws IOException {
        return new BlockBZIP2FileWriter(filenameBase, path, firstRecordOffset, chunkThreshold);
    }

    protected InputStream newCompressorInputStream(InputStream in) throws IOException {
        return new BZip2CompressorInputStream(in);
    }

    @Test
    public void testPaths() throws Exception {
        BlockFileWriter w = newBlockFileWriter("foo", tmpDir);
        assertEquals(tmpDir + "/foo-000000000000.bzip2", w.getDataFilePath());
        assertEquals(tmpDir + "/foo-000000000000.index.json", w.getIndexFilePath());


        BlockFileWriter w2 = newBlockFileWriter("foo", tmpDir, 123456);
        assertEquals(tmpDir + "/foo-000000123456.bzip2", w2.getDataFilePath());
        assertEquals(tmpDir + "/foo-000000123456.index.json", w2.getIndexFilePath());
    }

    @Test
    public void testWrite() throws Exception {
        // Very compressible 200 byte padding string to prepend to our unique line prefix
        String pad = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
                + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

        // Make a writer with artificially small chunk threshold of 1kb
        BlockFileWriter w = newBlockFileWriter("write-test", tmpDir, 987654321, 1000);

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
            w.write(line);
        }

        assertEquals(totalUncompressedBytes, w.getTotalUncompressedSize());
        assertEquals(50, w.getNumRecords());
        assertTrue("Should be at least 10 chunks in output file", w.getNumChunks() >= 10);

        w.close();

        verifyOutputIsSaneCompressedFile(w.getDataFilePath(), expectedLines);
        verifyIndexFile(w, 987654321, expectedLines);
    }



    // Hmm this test is actually not very conclusive - on OS X and most linux file systems
    // it passes anyway due to nature of filesystems. Not sure how to write something more robust
    // though to validate that we definitiely truncate the files even if we write less data

    @Test
    public void testShouldOverwrite() throws Exception {
        // Make writer and write to it a bit.
        {
            BlockBZIP2FileWriter w = new BlockBZIP2FileWriter("overwrite-test", tmpDir);

            // Write at least a few 4k blocks to disk so we can be sure that we don't
            // only overwrite the first block.
            String[] expectedLines = new String[5000];
            for (int i = 0; i < 5000; i++) {
                String line = String.format("Record %d", i);
                w.write(line);
                expectedLines[i] = line;
            }

            assertEquals(5000, w.getNumRecords());

            w.close();

            // Just check it actually write to disk
            verifyOutputIsSaneCompressedFile(w.getDataFilePath(), expectedLines);
            verifyIndexFile(w, 0, expectedLines);

        }

        {
            // Now make a whole new writer for same chunk
            BlockFileWriter w = newBlockFileWriter("overwrite-test", tmpDir);

            // Only write a few lines
            String[] expectedLines2 = new String[10];
            for (int i = 0; i < 10; i++) {
                String line = String.format("Overwrite record %d", i);
                w.write(line);
                expectedLines2[i] = line;
            }

            assertEquals(10, w.getNumRecords());

            w.close();

            // No check output is only the 10 lines we just wrote
            verifyOutputIsSaneCompressedFile(w.getDataFilePath(), expectedLines2);
            verifyIndexFile(w, 0, expectedLines2);
        }
    }

    @Test
    public void testDelete() throws Exception {
        // Make writer and write to it a bit.
        BlockFileWriter w = newBlockFileWriter("overwrite-test", tmpDir);

        String[] expectedLines = new String[5000];
        for (int i = 0; i < 5000; i++) {
            String line = String.format("Record %d", i);
            w.write(line);
            expectedLines[i] = line;
        }

        assertEquals(5000, w.getNumRecords());

        w.close();

        // Just check it actually write to disk
        verifyOutputIsSaneCompressedFile(w.getDataFilePath(), expectedLines);
        verifyIndexFile(w, 0, expectedLines);

        // Now remove it
        w.delete();

        File dataF = new File(w.getDataFilePath());
        File idxF = new File(w.getIndexFilePath());

        assertFalse("Data file should not exist after delete", dataF.exists());
        assertFalse("Index file should not exist after delete", idxF.exists());
    }
}

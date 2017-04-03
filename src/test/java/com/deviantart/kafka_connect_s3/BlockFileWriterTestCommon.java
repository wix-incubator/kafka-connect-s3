package com.deviantart.kafka_connect_s3;

import org.junit.Before;

import java.io.File;

public abstract class BlockFileWriterTestCommon {

    protected Class<BlockFileWriter> compressedFileWriterClass;

    protected static String tmpDir;

    @Before
    public void setUp() throws Exception {
        File f = new File(tmpDir);

        if (!f.exists()) {
            f.mkdir();
        }
    }
}

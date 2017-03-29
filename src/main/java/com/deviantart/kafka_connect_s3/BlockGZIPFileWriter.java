package com.deviantart.kafka_connect_s3;

import java.io.BufferedWriter;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * BlockGZIPFileWriter accumulates newline delimited UTF-8 records and writes them to an
 * output file that is readable by GZIP.
 *
 * In fact this file is the concatenation of possibly many separate GZIP files corresponding to smaller chunks
 * of the input. Alongside the output filename.gz file, a file filename-index.json is written containing JSON
 * metadata about the size and location of each block.
 *
 * This allows a reading class to skip to particular line/record without decompressing whole file by looking up
 * the offset of the containing block, seeking to it and beginning GZIp read from there.
 *
 * This is especially useful when the file is an archive in HTTP storage like Amazon S3 where GET request with
 * range headers can allow pulling a small segment from overall compressed file.
 *
 * Note that thanks to GZIP spec, the overall file is perfectly valid and will compress as if it was a single stream
 * with any regular GZIP decoding library or program.
 */
public class BlockGZIPFileWriter extends BlockFileWriter {

    private GZIPOutputStream gzipStream;

    public BlockGZIPFileWriter(String filenameBase, String path) throws FileNotFoundException, IOException {
        this(filenameBase, path, 0, 67108864);
    }

    public BlockGZIPFileWriter(String filenameBase, String path, long firstRecordOffset) throws FileNotFoundException, IOException {
        this(filenameBase, path, firstRecordOffset, 67108864);
    }

    public BlockGZIPFileWriter(String filenameBase, String path, long firstRecordOffset, long chunkThreshold) throws FileNotFoundException, IOException {
        super(filenameBase, path, firstRecordOffset, chunkThreshold);
    }

    @Override
    protected void initChunkWriter() throws IOException, UnsupportedEncodingException {
        gzipStream = new GZIPOutputStream(fileStream);
        writer = new BufferedWriter(new OutputStreamWriter(gzipStream, "UTF-8"));
    }

    @Override
    protected void finishChunk() throws IOException {
        Chunk ch = currentChunk();

        // Complete GZIP block without closing stream
        writer.flush();
        gzipStream.finish();

        // We can no find out how long this chunk was compressed
        long bytesWritten = fileStream.getNumBytesWritten();
        ch.compressedByteLength = bytesWritten - ch.byteOffset;
    }
}

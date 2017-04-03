package com.deviantart.kafka_connect_s3;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

/**
 * BlockBZIP2FileWriter accumulates newline delimited UTF-8 records and writes them to an
 * output file that is readable by BZIP2.
 *
 * In fact this file is the concatenation of possibly many separate BZIP2 files corresponding to smaller chunks
 * of the input. Alongside the output filename.gz file, a file filename-index.json is written containing JSON
 * metadata about the size and location of each block.
 *
 * This allows a reading class to skip to particular line/record without decompressing whole file by looking up
 * the offset of the containing block, seeking to it and beginning BZIP2 read from there.
 *
 * This is especially useful when the file is an archive in HTTP storage like Amazon S3 where GET request with
 * range headers can allow pulling a small segment from overall compressed file.
 *
 * Note that thanks to BZIP2 spec, the overall file is perfectly valid and will compress as if it was a single stream
 * with any regular BZIP2 decoding library or program.
 */
public class BlockBZIP2FileWriter extends BlockFileWriter {

    private BZip2CompressorOutputStream bzip2Stream;

    public BlockBZIP2FileWriter(String filenameBase, String path) throws FileNotFoundException, IOException {
        this(filenameBase, path, 0, 67108864);
    }

    public BlockBZIP2FileWriter(String filenameBase, String path, long firstRecordOffset) throws FileNotFoundException, IOException {
        this(filenameBase, path, firstRecordOffset, 67108864);
    }

    public BlockBZIP2FileWriter(String filenameBase, String path, long firstRecordOffset, long chunkThreshold) throws FileNotFoundException, IOException {
        super(filenameBase, path, firstRecordOffset, chunkThreshold);
    }

    @Override
    protected void initChunkWriter() throws IOException, UnsupportedEncodingException {
        bzip2Stream = new BZip2CompressorOutputStream(fileStream);
        writer = new BufferedWriter(new OutputStreamWriter(bzip2Stream, "UTF-8"));
    }

    @Override
    protected void finishChunk() throws IOException {
        Chunk ch = currentChunk();

        // Complete GZIP block without closing stream
        writer.flush();
        bzip2Stream.finish();

        // We can no find out how long this chunk was compressed
        long bytesWritten = fileStream.getNumBytesWritten();
        ch.compressedByteLength = bytesWritten - ch.byteOffset;
    }

    @Override
    public String getDataFileName() {
        return String.format("%s-%012d.bzip2", filenameBase, super.getFirstRecordOffset());
    }
}

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

abstract class BlockFileWriter {
    private String filenameBase;
    private String path;
    protected BufferedWriter writer;
    protected CountingOutputStream fileStream;

    protected ArrayList<Chunk> chunks;

    // Default each chunk is 64MB of uncompressed data
    private long chunkThreshold;

    // Offset to the first record.
    // Set to non-zero if this file is part of a larger stream and you want
    // record offsets in the index to reflect the global offset rather than local
    private long firstRecordOffset;

    BlockFileWriter(String filenameBase, String path, long firstRecordOffset, long chunkThreshold) throws IOException {
        this.filenameBase = filenameBase;
        this.path = path;
        this.firstRecordOffset = firstRecordOffset;
        this.chunkThreshold = chunkThreshold;

        chunks = new ArrayList<Chunk>();

        // Initialize first chunk
        Chunk ch = new Chunk();
        ch.firstOffset = firstRecordOffset;
        chunks.add(ch);

        // Explicitly truncate the file. On linux and OS X this appears to happen
        // anyway when opening with FileOutputStream but that behavior is not actually documented
        // or specified anywhere so let's be rigorous about it.
        FileOutputStream fos = new FileOutputStream(new File(getDataFilePath()));
        fos.getChannel().truncate(0);

        // Open file for writing and setup
        this.fileStream = new CountingOutputStream(fos);
        initChunkWriter();
    }

    abstract protected void initChunkWriter() throws IOException;
    abstract protected void finishChunk() throws IOException;

    protected Chunk currentChunk() {
        return chunks.get(chunks.size() - 1);
    }

    public long getFirstRecordOffset() {
        return firstRecordOffset;
    }

    public String getDataFileName() {
        return String.format("%s-%012d.gz", filenameBase, firstRecordOffset);
    }

    public String getIndexFileName() {
        return String.format("%s-%012d.index.json", filenameBase, firstRecordOffset);
    }

    public String getDataFilePath() {
        return String.format("%s/%s", path, this.getDataFileName());
    }

    public String getIndexFilePath() {
        return String.format("%s/%s", path, this.getIndexFileName());
    }

    /**
     * Writes string to file, assuming this is a single record
     *
     * If there is no newline at then end we will add one
     */
    public void write(String record) throws IOException {
        Chunk ch = currentChunk();

        boolean hasNewLine = record.endsWith("\n");

        int rawBytesToWrite = record.length();
        if (!hasNewLine) {
            rawBytesToWrite += 1;
        }

        if ((ch.rawBytes + rawBytesToWrite) > chunkThreshold) {
            finishChunk();
            initChunkWriter();

            Chunk newCh = new Chunk();
            newCh.firstOffset = ch.firstOffset + ch.numRecords;
            newCh.byteOffset = ch.byteOffset + ch.compressedByteLength;
            chunks.add(newCh);
            ch = newCh;
        }

        writer.append(record);
        if (!hasNewLine) {
            writer.newLine();
        }
        ch.rawBytes += rawBytesToWrite;
        ch.numRecords++;
    }

    public void delete() throws IOException {
        deleteIfExists(getDataFilePath());
        deleteIfExists(getIndexFilePath());
    }

    private void deleteIfExists(String path) throws IOException {
        File f = new File(path);
        if (f.exists() && !f.isDirectory()) {
            f.delete();
        }
    }

    public void close() throws IOException {
        // Flush last chunk, updating index
        finishChunk();
        // Now close the writer (and the whole stream stack)
        writer.close();
        writeIndex();
    }

    private void writeIndex() throws IOException {
        JSONArray chunkArr = new JSONArray();

        for (Chunk ch : chunks) {
            JSONObject chunkObj = new JSONObject();
            chunkObj.put("first_record_offset", ch.firstOffset);
            chunkObj.put("num_records", ch.numRecords);
            chunkObj.put("byte_offset", ch.byteOffset);
            chunkObj.put("byte_length", ch.compressedByteLength);
            chunkObj.put("byte_length_uncompressed", ch.rawBytes);
            chunkArr.add(chunkObj);
        }

        JSONObject index = new JSONObject();
        index.put("chunks", chunkArr);

        try (FileWriter file = new FileWriter(getIndexFilePath())) {
            file.write(index.toJSONString());
            file.close();
        }
    }

    public int getTotalUncompressedSize() {
        int totalBytes = 0;
        for (Chunk ch : chunks) {
            totalBytes += ch.rawBytes;
        }
        return totalBytes;
    }

    public int getNumChunks() {
        return chunks.size();
    }

    public int getNumRecords() {
        int totalRecords = 0;
        for (Chunk ch : chunks) {
            totalRecords += ch.numRecords;
        }
        return totalRecords;
    }
}

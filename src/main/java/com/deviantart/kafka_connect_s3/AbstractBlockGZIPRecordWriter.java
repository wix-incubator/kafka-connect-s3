package com.deviantart.kafka_connect_s3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public abstract class AbstractBlockGZIPRecordWriter implements BlockGZIPRecordWriter {
  protected String filenameBase;
  protected String path;
  protected GZIPOutputStream gzipStream;
  protected CountingOutputStream fileStream;
  protected ArrayList<Chunk> chunks;
  // Default each chunk is 64MB of uncompressed data
  protected long chunkThreshold;
  // Offset to the first record.
  // Set to non-zero if this file is part of a larger stream and you want
  // record offsets in the index to reflect the global offset rather than local
  protected long firstRecordOffset;

  public AbstractBlockGZIPRecordWriter(String filenameBase, String path, long chunkThreshold, long firstRecordOffset) throws IOException {
    this.filenameBase = filenameBase;
    chunks = new ArrayList<Chunk>();
    this.path = path;
    this.chunkThreshold = chunkThreshold;
    this.firstRecordOffset = firstRecordOffset;

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

  protected Chunk currentChunk() {
    return chunks.get(chunks.size() - 1);
  }

  public long getFirstRecordOffset() {
    return firstRecordOffset;
  }

  @Override
  public String getDataFileName() {
    return String.format("%s-%012d.gz", filenameBase, firstRecordOffset);
  }

  @Override
  public String getIndexFileName() {
    return String.format("%s-%012d.index.json", filenameBase, firstRecordOffset);
  }

  public String getDataFilePath() {
    return String.format("%s/%s", path, this.getDataFileName());
  }

  public String getIndexFilePath() {
    return String.format("%s/%s", path, this.getIndexFileName());
  }

  @Override
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

  protected void writeIndex() throws IOException {
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

  protected void initChunkWriter() throws IOException, UnsupportedEncodingException {
    gzipStream = new GZIPOutputStream(fileStream);
    initWriter();
  }

  protected abstract void initWriter() throws UnsupportedEncodingException;

  protected Chunk prepareChunk(int rawBytesToWrite) throws IOException {
    Chunk ch = currentChunk();

    if ((ch.rawBytes + rawBytesToWrite) > chunkThreshold) {
      finishChunk();
      initChunkWriter();

      Chunk newCh = new Chunk();
      newCh.firstOffset = ch.firstOffset + ch.numRecords;
      newCh.byteOffset = ch.byteOffset + ch.compressedByteLength;
      chunks.add(newCh);
      ch = newCh;
    }


    ch.rawBytes += rawBytesToWrite;
    ch.numRecords++;

    return ch;
  }

  protected void finishChunk() throws IOException {
    flushWriter();

    gzipStream.finish();

    Chunk ch = currentChunk();

    // We can no find out how long this chunk was compressed
    long bytesWritten = fileStream.getNumBytesWritten();
    ch.compressedByteLength = bytesWritten - ch.byteOffset;
  }

  protected abstract void flushWriter() throws IOException;

  @Override
  public void close() throws IOException {
    // Flush last chunk, updating index
    finishChunk();
    // Now close the writer (and the whole stream stack)
    closeWriter();
    writeIndex();
  }

  protected abstract void closeWriter() throws IOException;

  protected class Chunk {
    public long rawBytes = 0;
    public long byteOffset = 0;
    public long compressedByteLength = 0;
    public long firstOffset = 0;
    public long numRecords = 0;
  }

  protected class CountingOutputStream extends FilterOutputStream {
    private long numBytes = 0;

    CountingOutputStream(OutputStream out) throws IOException {
      super(out);
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
      numBytes++;
    }
    @Override
    public void write(byte[] b) throws IOException {
      out.write(b);
      numBytes += b.length;
    }
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
      numBytes += len;
    }

    public long getNumBytesWritten() {
      return numBytes;
    }
  }
}

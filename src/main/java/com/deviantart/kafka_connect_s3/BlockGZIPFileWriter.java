package com.deviantart.kafka_connect_s3;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
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
public class BlockGZIPFileWriter implements Closeable {

  private final Converter keyConverter; // nullable. key will be dropped
  private final Converter valueConverter; // nullable? value will be dropped

  private String filenameBase;
  private String path;
  private GZIPOutputStream gzipStream;
  private CountingOutputStream fileStream;

  private class Chunk {
    public long rawBytes = 0;
    public long byteOffset = 0;
    public long compressedByteLength = 0;
    public long firstOffset = 0;
    public long numRecords = 0;
  };

  private class CountingOutputStream extends FilterOutputStream {
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
  };

  private ArrayList<Chunk> chunks;

  // Default each chunk is 64MB of uncompressed data
  private long chunkThreshold;

  // Offset to the first record.
  // Set to non-zero if this file is part of a larger stream and you want
  // record offsets in the index to reflect the global offset rather than local
  private long firstRecordOffset;

  public BlockGZIPFileWriter(String filenameBase, String path) throws FileNotFoundException, IOException {
    this(filenameBase, path, 0, 67108864, null, defaultConverter());
  }

  public BlockGZIPFileWriter(String filenameBase, String path, long firstRecordOffset) throws FileNotFoundException, IOException {
    this(filenameBase, path, firstRecordOffset, 67108864, null, defaultConverter());
  }

  private static Converter defaultConverter() {
    return new ToStringWithDelimiterConverter();
  }

  public BlockGZIPFileWriter(String filenameBase, String path, long firstRecordOffset, long chunkThreshold) throws IOException {
    this(filenameBase, path, firstRecordOffset, chunkThreshold, null, defaultConverter());
  }

  public BlockGZIPFileWriter(String filenameBase, String path, long firstRecordOffset, long chunkThreshold, Converter keyConverter, Converter valueConverter)
  throws FileNotFoundException, IOException
  {
    this.filenameBase = filenameBase;
    this.path = path;
    this.firstRecordOffset = firstRecordOffset;
    this.chunkThreshold = chunkThreshold;
    this.keyConverter = keyConverter;
    this.valueConverter = valueConverter;

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

  private void initChunkWriter() throws IOException, UnsupportedEncodingException {
    gzipStream = new GZIPOutputStream(fileStream);
  }

  private Chunk currentChunk() {
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
  public void write(SinkRecord record) throws IOException {
    Chunk ch = currentChunk();

    List<byte[]> toWrite = new ArrayList<>(2);
    if (keyConverter != null) {
      toWrite.add(keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key()));
    }
    if (valueConverter != null) {
      toWrite.add(valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value()));
    }

    int rawBytesToWrite = 0;
    for (byte[] bytes : toWrite) {
     rawBytesToWrite += bytes.length;
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

    for (byte[] bytes : toWrite) {
      gzipStream.write(bytes);
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

  private void finishChunk() throws IOException {
    Chunk ch = currentChunk();

    // Complete GZIP block without closing stream
    gzipStream.finish();

    // We can no find out how long this chunk was compressed
    long bytesWritten = fileStream.getNumBytesWritten();
    ch.compressedByteLength = bytesWritten - ch.byteOffset;
  }

  public void close() throws IOException {
    // Flush last chunk, updating index
    finishChunk();
    // Now close the writer (and the whole stream stack)
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
package com.deviantart.kafka_connect_s3;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import org.apache.kafka.connect.sink.SinkRecord;

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
public class BlockGZIPStringWriter extends AbstractBlockGZIPRecordWriter {
  private BufferedWriter writer;

  public BlockGZIPStringWriter(String filenameBase, String path) throws FileNotFoundException, IOException {
    this(filenameBase, path, 0, 67108864);
  }

  public BlockGZIPStringWriter(String filenameBase, String path, long firstRecordOffset) throws FileNotFoundException, IOException {
    this(filenameBase, path, firstRecordOffset, 67108864);
  }

  public BlockGZIPStringWriter(String filenameBase, String path, long firstRecordOffset, long chunkThreshold)
  throws FileNotFoundException, IOException
  {
    super(filenameBase, path, chunkThreshold, firstRecordOffset);

  }

  @Override
  protected void initWriter() throws UnsupportedEncodingException {
    writer = new BufferedWriter(new OutputStreamWriter(gzipStream, "UTF-8"));
  }

  /**
   * Writes string to file, assuming this is a single record
   *
   * If there is no newline at then end we will add one
   */
  @Override
  public void write(SinkRecord rawRecord) throws IOException {
    write(rawRecord.value().toString());
  }

  // visible for testing
  void write(String record) throws IOException {
    boolean hasNewLine = record.endsWith("\n");

    int rawBytesToWrite = record.length();
    if (!hasNewLine) {
      rawBytesToWrite += 1;
    }

    prepareChunk(rawBytesToWrite);

    writer.append(record);
    if (!hasNewLine) {
      writer.append('\n');
    }
  }

  @Override
  protected void flushWriter() throws IOException {
    // Complete GZIP block without closing stream
    writer.flush();
  }

  @Override
  protected void closeWriter() throws IOException {
    writer.close();
  }

}
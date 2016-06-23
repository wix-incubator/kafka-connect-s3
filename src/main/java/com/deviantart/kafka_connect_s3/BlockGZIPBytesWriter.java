package com.deviantart.kafka_connect_s3;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Writes length prefixed raw key + value records. Lengths are encoded as a 4 byte, big endian integer.
 * Each record has the simple form: [key.length][key][value.length][value].
 */
public class BlockGZIPBytesWriter extends AbstractBlockGZIPRecordWriter {
  private static final int LENGTH_SIZE = 4; // 4 bytes to encode the length

  private final ByteBuffer lengthConverter = ByteBuffer.allocate(LENGTH_SIZE);

  public BlockGZIPBytesWriter(String filenameBase, String path, long firstRecordOffset, long chunkThreshold)
  throws IOException
  {
    super(filenameBase, path, chunkThreshold, firstRecordOffset);
  }

  @Override
  protected void initWriter() throws UnsupportedEncodingException {
    // gzip stream is already initialized
  }

  /**
   * Writes key & value to a file, assuming this is a single record.
   */
  @Override
  public void write(SinkRecord rawRecord) throws IOException {
    // keys are optional, so write a 0 if there isn't one
    byte[] key = rawRecord.key() == null ? new byte[0]
            : (byte[]) rawRecord.key();
    byte[] value = (byte[]) rawRecord.value();

    prepareChunk(LENGTH_SIZE + key.length + LENGTH_SIZE + value.length);

    writeBytesWithLength(key);
    writeBytesWithLength(value);
  }

  protected void writeBytesWithLength(byte[] bytes) throws IOException {
    lengthConverter.rewind();
    lengthConverter.putInt(bytes.length);
    gzipStream.write(lengthConverter.array());
    gzipStream.write(bytes);
  }

  @Override
  protected void flushWriter() throws IOException {
    // GZIP already flushed
  }

  @Override
  protected void closeWriter() throws IOException {
    // nothing to do
  }

}
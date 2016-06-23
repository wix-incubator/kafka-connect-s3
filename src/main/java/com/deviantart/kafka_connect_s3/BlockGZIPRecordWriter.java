package com.deviantart.kafka_connect_s3;

import java.io.Closeable;
import java.io.IOException;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Format agnostic interface for writing records to gzipped files.
 */
public interface BlockGZIPRecordWriter extends Closeable {
  String getDataFileName();

  String getIndexFileName();

  void write(SinkRecord record) throws IOException;

  void delete() throws IOException;

  int getNumRecords();

  String getDataFilePath();

  String getIndexFilePath();
}

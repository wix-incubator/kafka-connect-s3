package com.deviantart.kafka_connect_s3;

import static com.deviantart.kafka_connect_s3.S3SinkTask.s3client;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.joda.time.LocalDate;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Helpers for reading records out of S3. Not thread safe.
 * Records should be in order since S3 lists files in lexicographic order.
 * It is strongly recommended that you use a unique key prefix per topic as
 * there is no option to restrict this reader by topic.
 *
 * NOTE: hasNext() on the returned iterators may throw AmazonClientException if there
 * was a problem communicating with S3 or reading an object. Your code should
 * catch AmazonClientException and implement back-off and retry as desired.
 *
 * Any other exception should be considered a permanent failure.
 */
public class S3FilesReader implements Iterable<ConsumerRecord<byte[], byte[]>> {

	private final String bucket;
	private final String keyPrefix;
	private final AmazonS3 s3Client;

	private final BytesRecordReader reader = new BytesRecordReader();
	private final LocalDate sinceDate;
	private final int pageSize;


	public S3FilesReader(String bucket, String keyPrefix, AmazonS3 s3Client) {
		this(bucket, keyPrefix, s3Client, null, 500);
	}

	/**
	 * @param bucket the s3 bucket name
	 * @param keyPrefix prefix for keys, not including the date. Must match the prefix you configured the sink with.
	 * @param s3Client s3 client.
	 * @param sinceDate optional. The date to start from.
	 * @param pageSize number of s3 objects to load at a time.
	 */
	public S3FilesReader(String bucket, String keyPrefix, AmazonS3 s3Client, LocalDate sinceDate, int pageSize) {
		this.bucket = bucket;
		this.keyPrefix = keyPrefix == null ? ""
				: keyPrefix.endsWith("/") ? keyPrefix : keyPrefix + "/";
		this.s3Client = s3Client;
		this.sinceDate = sinceDate;
		// we have to filter out chunk indexes on this end, so
		// whatever the requested page size is, we'll need twice that
		this.pageSize = pageSize * 2;
	}

	public Iterator<ConsumerRecord<byte[], byte[]>> iterator() {
		return readFrom(sinceDate);
	}

	public Iterator<ConsumerRecord<byte[], byte[]>> readFrom(final LocalDate sinceDate) {
		return new Iterator<ConsumerRecord<byte[], byte[]>>() {
			ObjectListing lastObjectListing;
			ObjectListing objectListing;
			Iterator<S3ObjectSummary> nextFile = Collections.emptyIterator();
			Iterator<ConsumerRecord<byte[], byte[]>> iterator = Collections.emptyIterator();


			private void nextObject() {
				if (!nextFile.hasNext() && (objectListing == null || objectListing.isTruncated())) {
					ObjectListing last = this.objectListing;
					if (objectListing == null) {
						objectListing = s3Client.listObjects(new ListObjectsRequest(
								bucket,
								keyPrefix,
								// since the date is a prefix, we start with the first object on that day or later
								keyPrefix + datePrefix(sinceDate),
								null,
								pageSize
						));
					} else {
						objectListing = s3Client.listNextBatchOfObjects(objectListing);
					}
					lastObjectListing = last;
					List<S3ObjectSummary> chunks = new ArrayList<>(objectListing.getObjectSummaries().size() / 2);
					for (S3ObjectSummary chunk : objectListing.getObjectSummaries()) {
						if (chunk.getKey().endsWith(".gz")) {
							chunks.add(chunk);
						}
					}
					nextFile = chunks.iterator();
				}
				if (!nextFile.hasNext()) {
					iterator = Collections.emptyIterator();
					return;
				}
				try {
					iterator = reader.readAll(s3Client.getObject(bucket, nextFile.next().getKey())).iterator();
				} catch (IOException e) {
					throw new AmazonClientException(e);
				}
			}

			@Override
			public boolean hasNext() {
				if (!iterator.hasNext()) {
					nextObject();
				}
				return iterator.hasNext();
			}

			@Override
			public ConsumerRecord<byte[], byte[]> next() {
				return iterator.next();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	private static final SimpleDateFormat DATE = new SimpleDateFormat("yyyy-MM-dd");

	private String datePrefix(LocalDate date) {
		return date == null ? "" : DATE.format(date.toDate());
	}

	/**
	 * This is just for testing. It reads in all records
	 * from the s3 bucket configured by the properties file you give it
	 * and it writes out all the raw values. Unless your values all
	 * have a delimiter at the end, this is probably not useful to you.
	 */
	public static void main(String... args) throws IOException {
		if (args.length != 1) {
			System.err.println("Usage: java ...S3FileReader <config.properties>");
			System.exit(255);
		}
		Properties config = new Properties();
		config.load(new FileInputStream(args[0]));

		String bucket = config.getProperty("s3.bucket");
		String prefix = config.getProperty("s3.prefix");

		Map<String, String> props = new HashMap<>();
		for (Map.Entry<Object, Object> entry : config.entrySet()) {
			props.put(entry.getKey().toString(), entry.getValue().toString());
		}
		AmazonS3 client = s3client(props);

		S3FilesReader consumerRecords = new S3FilesReader(bucket, prefix, client);
		for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
			System.out.write(record.value());
		}
		System.out.close();
	}

}

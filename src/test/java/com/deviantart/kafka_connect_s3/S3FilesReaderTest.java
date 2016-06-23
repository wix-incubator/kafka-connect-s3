package com.deviantart.kafka_connect_s3;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.LocalDate;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListNextBatchOfObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import junit.framework.TestCase;

/**
 * Covers S3 and reading raw byte records.
 */
public class S3FilesReaderTest extends TestCase {

	public void testReadingBytesFromS3() throws IOException {
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenSomeData(dir);

		final AmazonS3 client = givenAMockS3Client(dir);

		List<String> results = whenTheRecordsAreRead(client);

		thenTheyAreFilteredAndInOrder(results);
	}

	private void thenTheyAreFilteredAndInOrder(List<String> results) {
		List<String> expected = Arrays.asList(
				"key0-0=value0-0",
				"key1-0=value1-0",
				"key1-1=value1-1"
		);
		assertEquals(expected, results);
	}

	private List<String> whenTheRecordsAreRead(AmazonS3 client) {
		S3FilesReader reader = new S3FilesReader(
				"bucket",
				"prefix",
				client,
				new LocalDate(2016, 1, 1),
				1 // small to test pagination
		);
		List<String> results = new ArrayList<>();
		for (ConsumerRecord<byte[], byte[]> record : reader) {
			results.add(new String(record.key()) + "="  + new String(record.value()));
		}
		return results;
	}

	private AmazonS3 givenAMockS3Client(final Path dir) {
		final AmazonS3 client = mock(AmazonS3Client.class);
		when(client.listObjects(any(ListObjectsRequest.class))).thenAnswer(new Answer<ObjectListing>() {
			@Override
			public ObjectListing answer(InvocationOnMock invocationOnMock) throws Throwable {
				final ListObjectsRequest req = (ListObjectsRequest) invocationOnMock.getArguments()[0];
				ObjectListing listing = mock(ObjectListing.class);

				final List<File> files = new ArrayList<>();
				Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult preVisitDirectory(Path toCheck, BasicFileAttributes attrs) throws IOException {
						if (toCheck.startsWith(dir)) {
							return FileVisitResult.CONTINUE;
						}
						return FileVisitResult.SKIP_SUBTREE;
					}

					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
						String key = key(file.toFile());
						if (req.getMarker() == null
								|| key.compareTo(req.getMarker()) > 0) {
							files.add(file.toFile());
						}
						return FileVisitResult.CONTINUE;
					}
				});

				List<S3ObjectSummary> summaries = new ArrayList<>();
				for (int i = 0; i < req.getMaxKeys() && i < files.size(); i++) {
					S3ObjectSummary summary = mock(S3ObjectSummary.class);
					String key = key(files.get(i));
					when(summary.getKey()).thenReturn(key);
					when(listing.getNextMarker()).thenReturn(key);
					summaries.add(summary);
				}

				when(listing.getMaxKeys()).thenReturn(req.getMaxKeys());

				when(listing.getObjectSummaries()).thenReturn(summaries);
				when(listing.isTruncated()).thenReturn(files.size() > req.getMaxKeys());

				return listing;
			}

			private String key(File file) {
				return file.getAbsolutePath().substring(dir.toAbsolutePath().toString().length() + 1);
			}
		});
		when(client.listNextBatchOfObjects(any(ObjectListing.class))).thenCallRealMethod();
		when(client.listNextBatchOfObjects(any(ListNextBatchOfObjectsRequest.class))).thenCallRealMethod();

		when(client.getObject(anyString(), anyString())).thenAnswer(new Answer<S3Object>() {
			@Override
			public S3Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				String key = (String) invocationOnMock.getArguments()[1];
				S3Object obj = mock(S3Object.class);
				File file = new File(dir.toString(), key);
				when(obj.getKey()).thenReturn(file.getName());
				when(obj.getObjectContent()).thenReturn(new S3ObjectInputStream(new FileInputStream(file), null));
				return obj;
			}
		});
		return client;
	}

	private void givenSomeData(Path dir) throws IOException {
		new File(dir.toFile(), "prefix/2015-12-31").mkdirs();
		new File(dir.toFile(), "prefix/2016-01-01").mkdirs();
		new File(dir.toFile(), "prefix/2016-01-02").mkdirs();
		try (BlockGZIPRecordWriter writer0 = new BlockGZIPBytesWriter("topic-00000", dir.toString() + "/prefix/2015-12-31", 0, 512);
			 BlockGZIPRecordWriter writer1 = new BlockGZIPBytesWriter("topic-00000", dir.toString() + "/prefix/2016-01-01", 0, 512);
			 BlockGZIPRecordWriter writer2 = new BlockGZIPBytesWriter("topic-00001", dir.toString() + "/prefix/2016-01-02", 0, 512);
		) {
			writer0.write(new SinkRecord(
					"topic",
					0,
					Schema.BYTES_SCHEMA, "willbe".getBytes(),
					Schema.BYTES_SCHEMA, "skipped".getBytes(),
					-1
			));

			writer1.write(new SinkRecord(
					"topic",
					0,
					Schema.BYTES_SCHEMA, "key0-0".getBytes(),
					Schema.BYTES_SCHEMA, "value0-0".getBytes(),
					0
			));

			writer2.write(new SinkRecord(
					"topic",
					1,
					Schema.BYTES_SCHEMA, "key1-0".getBytes(),
					Schema.BYTES_SCHEMA, "value1-0".getBytes(),
					0
			));
			writer2.write(new SinkRecord(
					"topic",
					1,
					Schema.BYTES_SCHEMA, "key1-1".getBytes(),
					Schema.BYTES_SCHEMA, "value1-1".getBytes(),
					1
			));
		}
	}

}

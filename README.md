# Kafka Connect Cloud Storage Sink Connector
[![build passing](https://jenkins-public.personali.io/badge-icon?job=personali/kafka-connect-s3/master)](http://172.34.1.161:8080/view/Organization/job/personali/job/kafka-connect-s3/job/master/)

This is a [kafka-connect](http://kafka.apache.org/documentation.html#connect) sink for Cloud Storages such as S3/GCS, without any dependency on HDFS/Hadoop libraries or data formats.

## Origin Fork
This repository is a fork of [DeviantArt/kafka-connect-s3](https://github.com/DeviantArt/kafka-connect-s3)

### Main differences from the origin repository

- Support of a couple of Cloud Storage provides. (Currently GCS and S3)
- Storage partitioning by a date field of the message
- Customizable storage partitioning. e.g.

    "custom.date.partition.format":"yyyy/MM/dd" will yield a partitions as gcs:[bucket_name] /2017/12/20/file

    "custom.date.partition.format":"yyyy-MM-dd" will yield a partitions as gcs:[bucket_name] /2017-12-20/file
- Supports plug able FlushAdditionalTask. This for example lets you sign the name of a new uploaded file, it's source topic and storage location in a database table so you can later on process that file.
  An implementation of registering the new files in a management table on BigQuery and Redshift is available.
## Status

This is pre-production code. Use at your own risk.

That said, we've put some effort into a reasonably thorough test suite and will be putting it into production shortly. We will update this notice when we have it running smoothly in production.

If you use it, you will likely find issues or ways it can be improved. Please feel free to create pull requests/issues and we will do our best to merge anything that helps overall (especially if it has passing tests ;)).

This was built against Kafka 0.10.1.1.

## Block-GZIP Output Format

This format is essentially just a GZIPed text file with one Kafka message per line.

It's actually a little more sophisticated than that though. We exploit a property of GZIP whereby multiple GZIP encoded files can be concatenated to produce a single file. Such a concatenated file is a valid GZIP file in its own right and will be decompressed by _any GZIP implementation_ to a single stream of lines -- exactly as if the input files were concatenated first and compressed together.

### Rationale

This allows us to keep chunk files large which is good for the most common batch/ETL workloads, while enabling relatively efficient access if needed.

If we sized each file at say 64MB then operating on a week's data might require downloading 10s or 100s of thousands of separate S3 objects which has non-trivial overhead compared to a few hundred. It also costs more to list bucket contents when you have millions of objects for a relatively short time period since S3 list operations can only return 1000 at once. So we wanted chunks around 1GB in size for most cases.

But in some rarer cases, we might want to resume processing from a specific offset in middle of a block, or pull just a few records at a specific offset. Rather than pull the whole 1GB chunk each time we need that, this scheme allows us to quickly find a much smaller chunk to satisfy the read.

We use a chunk threshold of 64MB by default although it's configurable. Keep in mind this is 64MB of _uncompressed_ input records, so the actual block within the file is likely to be significantly smaller depending on how compressible your data is.

Also note that while we don't anticipate random reads being common, this format is very little extra code and virtually no size/performance overhead compared with just GZIPing big chunks so it's a neat option to have.

### Example

We output 2 files per chunk

```
$ tree system_test/data/connect-system-test/systest/2016-02-24/
system_test/data/connect-system-test/systest/2016-02-24/
├── system-test-00000-000000000000.gz
├── system-test-00000-000000000000.index.json
├── system-test-00000-000000000100.gz
├── system-test-00000-000000000100.index.json
├── system-test-00000-000000000200.gz
├── system-test-00000-000000000200.index.json
├── system-test-00001-000000000000.gz
├── system-test-00001-000000000000.index.json
├── system-test-00002-000000000000.gz
└── system-test-00002-000000000000.index.json
```
 - the actual *.gz file which can be read and treated as a plain old gzip file on it's own
 - a *.index.json file which described the byte positions of each "block" inside the file

If you don't care about seeking to specific offsets efficiently then ignore the index files and just use the *.gz as if it's a plain old gzip file.

Note file name format is: `<topic name>-<zero-padded partition>-<zero-padded offset of first record>.*`. That implies that if you exceed 10k partitions in a topic or a trillion message in a single partition, the files will no longer sort naturally. In practice that is probably not a big deal anyway since we prefix with upload date too to make listing recent files easier. Making padding length configurable is an option. It's mostly makes things simpler to eyeball with low numbers where powers of ten change fast anyway.

If you want to have somewhat efficient seeking to particular offset though, you can do it like this:
 - List bucket contents and locate the chunk that the offset is in
 - Download the `*.index.json` file for that chunk, it looks something like this (note these are artificially small chunks):
```
$ cat system-test-00000-000000000000.index.json | jq -M '.'
{
  "chunks": [
    {
      "byte_length_uncompressed": 2890,
      "num_records": 100,
      "byte_length": 275,
      "byte_offset": 0,
      "first_record_offset": 0
    },
    {
      "byte_length_uncompressed": 3121,
      "num_records": 123,
      "byte_length": 325,
      "byte_offset": 275,
      "first_record_offset": 100
    },
    ...
  ]
}
```
 - Iterate through the "chunks" described in the index. Each has a `first_record_offset` and `num_records` so you can work out if the offset you want to find is in that chunk.
  - `first_record_offset` is the absolute kafka topic-partition offset of the first message in the chunk. Hence the first chunk in the index will always have the same `first_record_offset` as the offset in the file name - `0` in this case.
 - When you've found the correct chunk, use the `byte_offset` and `byte_length` fields to make a [range request](https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35) to S3 to download only the block you care about.
  - Depending on your needs you can either limit to just the single block, or if you want to consume all records after that offset, you can consume from the offset right to the end of the file
 - The range request bytes can be decompressed as a GZIP file on their own with any GZIP compatible tool, provided you limit to whole block boundaries.

## Build and Run

### Dockerized

Add Additional resources to the additional_resources folder (Google credentials files, Redshift JDBC Driver...)

Build Image:
```mvn package -Dskip.docker=false -Ddocker.repo=your-repo/kafka-connect-cloud-storage```

Edit the docker-compose-example.yml and setup at least you kafka endpoints.

Start container:
```docker-compose -f docker-compose-example.yml```

Edit the connector-exapmle.json with your settings.

Submit the rest api call to start the connector:
```
curl -H "Content-Type: application/json" \
   --data "@connector-example.json" \
   http://localhost:8083/connectors
```

### Not Dockerized

You should be able to build this with `mvn package`. Once the jar is generated in target folder include it in  `CLASSPATH` (ex: for Mac users,export `CLASSPATH=.:$CLASSPATH:/fullpath/to/kafka-connect-cloud-storage.jar` )

Run: `bin/connect-standalone.sh  example-connect-worker.properties example-connect-s3-sink.properties`(from the root directory of project, make sure you have kafka on the path, if not then give full path of kafka before `bin`)

## Configuration

In addition to the [standard kafka-connect config options](http://kafka.apache.org/documentation.html#connectconfigs) we support/require the following, in the task properties file or REST config:

| Config Key | Default | Notes |
| ---------- | ------- | ----- |
| storage.class | com.personali.kafka.connect.cloud.storage.storage.GCSStorageWriter | The name of the Storage handler class.  GCSStorageWriter/S3StorageWriter |
| storage.partition.class | **REQUIRED** | The name of the Storage partitioning class.  com.personali.kafka.connect.cloud.storage.partition.CustomDateFormatStoragePartition |
| additional.flush.task.class | | The name of the flush additional task class. Currently available: com.personali.kafka.connect.cloud.storage.flush.FlushAdditionalBigQueryRegisterTask , com.personali.kafka.connect.cloud.storage.flush.FlushAdditionalRedshiftRegisterTask
| storage.bucket | **REQUIRED** | The name of the bucket to write too. |
| local.buffer.dir | **REQUIRED** | Directory to store buffered data in. Must exist. |
| [redshift/bigquery/mysql].table.name | | A table name for registering new uploaded files |
| [redshift/bigquery/mysql].register.[topic_name]| `false` | Should we register files related to [topic_name] |
| redshift.user | | Redshift user name when using FlushAdditionalRedshiftRegisterTask |
| redshift.password | | Redshift password when using FlushAdditionalRedshiftRegisterTask |
| redshift.connection.url | | Redshift connection url when using FlushAdditionalRedshiftRegisterTask |
| mysql.jdbc.url | | Mysql JDBC url when using FlushAdditionalMysqlRegisterTask |
| mysql.user | | Mysql JDBC user when using FlushAdditionalMysqlRegisterTask |
| mysql.password | | Mysql JDBC password when using FlushAdditionalMysqlRegisterTask |
| storage.prefix | `""` | Prefix added to all object keys stored in bucket to "namespace" them. |
| s3.endpoint | AWS defaults per region | Mostly useful for testing. |
| s3.path_style | `false` | Force path-style access to bucket rather than subdomain. Mostly useful for tests. |
| compressed_block_size | 67108864 | How much _uncompressed_ data to write to the file before we rol to a new block/chunk. See [Block-GZIP](#user-content-block-gzip-output-format) section above. |

## Credentials & Connectivity for cloud services
### S3
Note that we use the default AWS SDK credentials provider. [Refer to their docs](http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html#id1) for the options for configuring S3 credentials.

### Redshift
Redshift credentials is configured by redshift.user, redshift.password, redshift.connection.url config params.
Redshift JDBC Driver should be available at the classpath.

### GCS & Google BigQuery
Credentials to google cloud services are supplied using [Google Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials)

## Testing

Integration tests can be ran using Docker.
Simply run the `run-integration-tests.sh` && `run-integration-tests-mysql.sh` scripts on a Mac/Linux system.

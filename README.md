# Kafka Connect S3 Sink

This is a [kafka-connect](http://kafka.apache.org/documentation.html#connect) sink for Amazon S3, without any dependency on HDFS/Hadoop libraries or data formats.

## Status

This is pre-production code. Use at your own risk.

That said, we've put some effort into a reasonably thorough test suite and will be putting it into production shortly. We will update this notice when we have it running smoothly in production.

If you use it, you will likely find issues or ways it can be improved. Please feel free to create pull requests/issues and we will do our best to merge anything that helps overall (especially if it has passing tests ;)).

This was built against Kafka 0.10.1.1.

## Block-GZIP Output Format

For now there is just one output format which is essentially just a GZIPed text file with one Kafka message per line.

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

## Other Formats

For now we only support Block-GZIP output. This assumes that all your kafka messages can be output as newline-delimited text files.

We could make the output format pluggable if others have use for this connector, but need binary serialisation formats like Avro/Thrift/Protobuf etc. Pull requests welcome.

## Build and Run

You should be able to build this with `mvn package`. Once the jar is generated in target folder include it in  `CLASSPATH` (ex: for Mac users,export `CLASSPATH=.:$CLASSPATH:/fullpath/to/kafka-connect-s3-jar` )

Run: `bin/connect-standalone.sh  example-connect-worker.properties example-connect-s3-sink.properties`(from the root directory of project, make sure you have kafka on the path, if not then give full path of kafka before `bin`)


There is a script `local-run.sh` which you can inspect to see how to get it running. This script relies on having a local kafka instance setup as described in testing section below.

## Configuration

In addition to the [standard kafka-connect config options](http://kafka.apache.org/documentation.html#connectconfigs) we support/require the following, in the task properties file or REST config:

| Config Key | Default | Notes |
| ---------- | ------- | ----- |
| s3.bucket | **REQUIRED** | The name of the bucket to write too. |
| local.buffer.dir | **REQUIRED** | Directory to store buffered data in. Must exist. |
| s3.prefix | `""` | Prefix added to all object keys stored in bucket to "namespace" them. |
| s3.endpoint | AWS defaults per region | Mostly useful for testing. |
| s3.path_style | `false` | Force path-style access to bucket rather than subdomain. Mostly useful for tests. |
| compressed_block_size | 67108864 | How much _uncompressed_ data to write to the file before we rol to a new block/chunk. See [Block-GZIP](#user-content-block-gzip-output-format) section above. |

Note that we use the default AWS SDK credentials provider. [Refer to their docs](http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html#id1) for the options for configuring S3 credentials.

## Testing

Most of the custom logic for handling output formatting, and managing S3 has reasonable mocked unit tests. There are probably improvements that can be made, but the logic is not especially complex.

There is also a basic system test to validate the integration with kafka-connect. This is not complete nor is it 100% deterministic due to the vagaries of multiple systems with non-deterministic things like timers effecting behaviour.

But it does consistently pass when run by hand on my Mac and validates basic operation of:

 - Initialisation and consuming/flushing all expected data
 - Resuming correctly on restart based on S3 state not relying on local disk state
 - Reconfiguring partitions of a topic and correctly resuming each

It doesn't test distributed mode operation yet, however the above is enough to exercise all of the integration points with the kafka-connect runtime.

### System Test Setup

See [the README in the system_test dir](/system_test/README.md) for details on setting up dependencies and environment to run the tests.

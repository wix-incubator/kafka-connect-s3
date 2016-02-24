# Kafka Connect S3 Sink

This is a [kafka-connect](http://kafka.apache.org/documentation.html#connect) sink for Amazon S3, but without any dependency on HDFS/hadoop libs or data formats.

## Status

This is pre-production code. Use at your own risk.

That said, we've put some effort into a reasonably thorough test suite and I will be putting it into production shortly. Will update this notice when we have it running smoothly in production.

If you use it, you will likely find issues or improvements, please feel free to create pull requests/issues and we will do our best to merge anything that helps overall (especially if it has passing tests ;)).

## Block-GZIP Output Format

For now there is just one output format which is essentially just a GZIPed text file per chunk with one Kafka message per line.

It's actually a little more sophisticated than that though. We exploit a property of GZIP whereby multiple GZIP encoded files can be concatenated to produce a single file. Such a concatenated file is a valid GZIP file in its own right and will be decompressed by _any GZIP implementation_ to a single stream of lines -- exactly as if the input files were concatenated first and compressed together.

So we output 2 files per chunk:
 - the actual *.gz file which can be read and treated as a plain old gzip file on it's own
 - a *.index.json file which described the byte positions of each "block" inside the file

If you don't care about seeking to specific offsets efficiently then ignore the index files and just use the *.gz as if it's a plain old gzip file.

If you want to have somewhat efficient seeking to particular offset though, you can do it like this:
 - List bucket contents and locate the chunk that the offset is in
 - Download the `*.index.json` file for that chunk
 - Iterate through the "chunks" described in the index. Each has a `first_record_offset` and `num_records` so you can work out if the offset you want to find is in that chunk.
 - When you've found the correct chunk, use the `byte_offset` and `byte_length` fields to make a [range request](https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35) to S3 to download only the block you care about.
  - Depending on your needs you can either limit to just the single block, or if you want to consume all records after that offset, you can consume from the offset right to the end of the file
 - The range request bytes can be decompressed as a GZIP file on their own with any GZIp compatible tool, provided you limit to whole block boundaries.

For now it assumes the kafka messages can be output as newline-delimited text files. We could make the output format pluggable if others have use for this connector.

## Build and Run

You should be able to build this with `mvn package`.

There is a script `local-run.sh` which you can inspect to see how to get it running. That relies on local kafka instance setup described in testing section below.

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
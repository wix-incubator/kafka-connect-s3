package com.deviantart.kafka_connect_s3;

class Chunk {
    public long rawBytes = 0;
    public long byteOffset = 0;
    public long compressedByteLength = 0;
    public long firstOffset = 0;
    public long numRecords = 0;
};

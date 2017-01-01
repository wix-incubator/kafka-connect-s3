package com.deviantart.kafka_connect_s3.partition;

/**
 * Created by or on 27/12/16.
 */
public interface PartitionKey {
    public String getPartitionPath();
}

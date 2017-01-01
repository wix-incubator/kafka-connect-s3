package com.deviantart.kafka_connect_s3.partition;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Created by or on 27/12/16.
 */
public abstract class TopicPartitionKey {
    private final String topic;
    private final Integer partition;
    private int hash = 0;

    public TopicPartitionKey(SinkRecord record) {
        this.topic = record.topic();
        this.partition = record.kafkaPartition();
    }

    public String getTopic() {
        return topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public int hashCode() {
        if(this.hash != 0) {
            return this.hash;
        } else {
            boolean prime = true;
            byte result = 1;
            int result1 = 31 * result + this.partition;
            result1 = 31 * result1 + (this.topic == null?0:this.topic.hashCode());
            this.hash = result1;
            return result1;
        }
    }

    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        } else if(obj == null) {
            return false;
        } else if(this.getClass() != obj.getClass()) {
            return false;
        } else {
            TopicPartitionKey other = (TopicPartitionKey)obj;
            if(this.partition != other.partition) {
                return false;
            } else {
                if(this.topic == null) {
                    if(other.topic != null) {
                        return false;
                    }
                } else if(!this.topic.equals(other.topic)) {
                    return false;
                }

                return true;
            }
        }
    }

    public String toString() {
        return this.topic + "-" + this.partition;
    }
}

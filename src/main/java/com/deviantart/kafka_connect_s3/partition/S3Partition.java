package com.deviantart.kafka_connect_s3.partition;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.sink.SinkRecord;

import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * Created by or on 27/12/16.
 */
public abstract class S3Partition {
    private final String topic;
    private final Integer partition;
    private int hash = 0;

    public S3Partition(SinkRecord record, Map<String,String> props){
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
            S3Partition other = (S3Partition)obj;
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

    public String getDataDirectory(){
        //Simple partitioning by upload date
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return String.format("%s/", df.format(new Date()));
    }

    public String getIndexDirectory(){
        //Simple partitioning by upload date
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return String.format("%s/", df.format(new Date()));
    }

    public String getDataFileName(){
        return String.format("%s-%05d", this.topic, this.partition);
    }

    public String getIndexFileName(){
        return String.format("%s-%05d.index.json", this.topic, this.partition);
    }


}

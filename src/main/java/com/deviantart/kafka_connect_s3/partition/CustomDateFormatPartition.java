package com.deviantart.kafka_connect_s3.partition;

import org.apache.kafka.connect.sink.SinkRecord;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by or on 27/12/16.
 */
public class CustomDateFormatPartition extends TopicPartitionKey implements PartitionKey {

    private Date partitionDate = new Date();
    private String dateFormat = "yyyy-MM-dd";

    public CustomDateFormatPartition(SinkRecord record) {
        super(record);
    }

    public CustomDateFormatPartition(SinkRecord record, String dateFormat) {
        super(record);
        this.dateFormat = dateFormat;
    }

    public CustomDateFormatPartition(SinkRecord record, String dateFormat, Date partitionDate) {
        super(record);
        this.dateFormat = dateFormat;
        this.partitionDate = partitionDate;
    }

    @Override
    public String getPartitionPath() {
        SimpleDateFormat df = new SimpleDateFormat(dateFormat);
        return df.format(partitionDate);
    }

    public Date getPartitionDate() {
        return partitionDate;
    }

    public void setPartitionDate(Date partitionDate) {
        this.partitionDate = partitionDate;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

}

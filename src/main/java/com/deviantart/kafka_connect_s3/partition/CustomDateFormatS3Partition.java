package com.deviantart.kafka_connect_s3.partition;

import com.deviantart.kafka_connect_s3.parser.JsonRecordParser;
import org.apache.kafka.connect.sink.SinkRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by or on 27/12/16.
 */
public class CustomDateFormatS3Partition extends S3Partition {

    private Date partitionDate;
    private String dateFormat = "yyyy/MM/dd";

    public CustomDateFormatS3Partition(SinkRecord record, Map<String,String> props) {
        super(record,props);
        this.dateFormat = props.get("custom.date.partition.format");
        String dateField = props.get("custom.date.field");
        String dateFieldFormat = props.get("custom.date.field.format");
        try {
            this.partitionDate = new JsonRecordParser().getDateField((String) record.value(), dateField, dateFieldFormat);
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public String getDataDirectory() {
        SimpleDateFormat df = new SimpleDateFormat(dateFormat);
        return String.format("%s/", df.format(partitionDate));
    }

    @Override
    public String getIndexDirectory(){
        SimpleDateFormat df = new SimpleDateFormat(dateFormat);
        return String.format("indexes/%s/", df.format(partitionDate));
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

    @Override
    public boolean equals(Object obj){
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        if (!super.equals(obj)) { return false; }
        else {
            CustomDateFormatS3Partition comparedObj = (CustomDateFormatS3Partition)obj;
            if ( this.getDataDirectory().equals(comparedObj.getDataDirectory()) &&
                   this.getDataFileName().equals(comparedObj.getDataFileName())){
                return true;
            }
            return false;
        }
    }

    public String toString() {
        return super.toString() + this.getDataDirectory();
    }

}

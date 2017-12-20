package com.personali.kafka.connect.cloud.storage.partition;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Created by orsher on 1/4/17.
 */
@RunWith(MockitoJUnitRunner.class)
public class CustomDateFormatS3PartitionTest {
    Map<String,String> config;

    @Mock
    SinkRecord record;

    @Before
    public void setUp() throws Exception {
        config = new HashMap<>();
        config.put("custom.date.partition.format","yyyy/MM/dd");
        config.put("custom.date.field","event_time");
        config.put("custom.date.field.format","yyyy-MM-dd HH:mm:ss");
    }

    @Test
    public void TestSuccessfulPartition(){
        when(record.value()).thenReturn("{\"id\":1, \"event_time\":\"2016-01-02 10:00:00\"}");
        CustomDateFormatStoragePartition s3Partition = new CustomDateFormatStoragePartition(record,config);
        assertEquals(s3Partition.getDataDirectory(),"2016/01/02/");
        assertEquals(s3Partition.getIndexDirectory(),"indexes/2016/01/02/");

        config.put("custom.date.partition.format","yyyy-dd-MM");
        s3Partition = new CustomDateFormatStoragePartition(record,config);
        assertEquals(s3Partition.getDataDirectory(),"2016-02-01/");
        assertEquals(s3Partition.getIndexDirectory(),"indexes/2016-02-01/");
    }

    @Test
    public void TestFailedPartition(){
        when(record.value()).thenReturn("2016-01-02 10:00:00    tab separated   data");
        SimpleDateFormat df = new SimpleDateFormat(config.get("custom.date.partition.format"));
        CustomDateFormatStoragePartition s3Partition = new CustomDateFormatStoragePartition(record,config);
        String todaysPartitionFormat = df.format(new Date());
        assertEquals(s3Partition.getDataDirectory(),"failures/"+todaysPartitionFormat+"/");
        assertEquals(s3Partition.getIndexDirectory(),"failures/indexes/"+todaysPartitionFormat+"/");
    }
}

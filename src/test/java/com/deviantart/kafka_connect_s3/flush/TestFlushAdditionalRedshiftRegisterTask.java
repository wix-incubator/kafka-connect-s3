package com.deviantart.kafka_connect_s3.flush;

import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestFlushAdditionalRedshiftRegisterTask {

  Map<String,String> config;
  Map<TopicPartition,ArrayList<String>> s3DataFileKeys;

  @Mock
  Connection mockConnection;
  @Mock
  Statement mockStatement;

  @Before
  public void setUp() throws Exception {
      when(mockConnection.createStatement()).thenReturn(mockStatement);
      config = new HashMap<>();
      config.put("redshift.table.name","event_files_merge_manager");
      config.put("redshift.register.topic1","true");
      config.put("redshift.register.topic2","true");
      config.put("redshift.register.topic3","false");
      config.put("s3.bucket","test.bucket");
      s3DataFileKeys = new HashMap<>();
      s3DataFileKeys.put(new TopicPartition("topic1",1),new ArrayList<>(Arrays.asList("path/to/file1.gz", "path/to/file/number/2.gz")));
      s3DataFileKeys.put(new TopicPartition("topic1",2),new ArrayList<>(Arrays.asList("path/to/file/number/3.gz")));
      s3DataFileKeys.put(new TopicPartition("topic2",1),new ArrayList<>(Arrays.asList("topic2/path/to/file1.gz", "topic2/path/to/file/number/2.gz")));
      s3DataFileKeys.put(new TopicPartition("topic3",1),new ArrayList<>(Arrays.asList("topic2/path/to/file1.gz", "topic2/path/to/file/number/2.gz")));
      s3DataFileKeys.put(new TopicPartition("topic4",1),new ArrayList<>(Arrays.asList("topic2/path/to/file1.gz", "topic2/path/to/file/number/2.gz")));
  }

  @Test
  public void testRun() throws Exception {
      FlushAdditionalRedshiftRegisterTask taskSpy = spy(new FlushAdditionalRedshiftRegisterTask());
      doReturn(mockConnection).when(taskSpy).getConnection(config);
      taskSpy.run(s3DataFileKeys,config);
      verify(mockStatement, times(1)).execute("INSERT INTO event_files_merge_manager VALUES ('topic1','path/to/file1.gz','UPLOADED',getdate())");
      verify(mockStatement, times(1)).execute("INSERT INTO event_files_merge_manager VALUES ('topic1','path/to/file/number/2.gz','UPLOADED',getdate())");
      verify(mockStatement, times(1)).execute("INSERT INTO event_files_merge_manager VALUES ('topic1','path/to/file/number/3.gz','UPLOADED',getdate())");
      verify(mockStatement, times(1)).execute("INSERT INTO event_files_merge_manager VALUES ('topic2','topic2/path/to/file1.gz','UPLOADED',getdate())");
      verify(mockStatement, times(1)).execute("INSERT INTO event_files_merge_manager VALUES ('topic2','topic2/path/to/file/number/2.gz','UPLOADED',getdate())");
      verify(mockStatement, times(5)).execute(anyString());
      verify(mockConnection, times(1)).commit();
  }
}

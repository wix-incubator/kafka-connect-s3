package com.personali.kafka.connect.cloud.storage.flush;

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
public class TestFkushAdditionalSFRegisterTask {

    Map<String,String> config;
    Map<TopicPartition, ArrayList<String>> storageDataFileKeys;

    @Mock
    Connection mockConnection;
    @Mock
    Statement mockStatement;

    @Before
    public void setUp() throws Exception {
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        config = new HashMap<>();
        config.put("s3.bucket","test.bucket");
        config.put("snowflake.jdbc.url","jdbc:snowflake://rs23589.east-us-2.azure.snowflakecomputing.com/");
        config.put("snowflake.user","PYTHON_WORKER");
        config.put("snowflake.password","Bnudu2020");
        config.put("snowflake.schema","PERSONALI");
        config.put("snowflake.db","DATA_DB");

        config.put("snowflake.register.topic1","true");
        config.put("snowflake.register.topic2","true");
        config.put("snowflake.table.name","event_files_merge_manager");

        storageDataFileKeys = new HashMap<>();
        storageDataFileKeys.put(new TopicPartition("topic1",1),new ArrayList<>(Arrays.asList("path/to/file1.gz", "path/to/file/number/2.gz")));
        storageDataFileKeys.put(new TopicPartition("topic1",2),new ArrayList<>(Arrays.asList("path/to/file/number/3.gz")));
        storageDataFileKeys.put(new TopicPartition("topic2",1),new ArrayList<>(Arrays.asList("topic2/path/to/file1.gz", "topic2/path/to/file/number/2.gz")));
        storageDataFileKeys.put(new TopicPartition("topic3",1),new ArrayList<>(Arrays.asList("topic2/path/to/file1.gz", "topic2/path/to/file/number/2.gz")));
        storageDataFileKeys.put(new TopicPartition("topic4",1),new ArrayList<>(Arrays.asList("topic2/path/to/file1.gz", "topic2/path/to/file/number/2.gz")));
    }

    @Test
    public void testRun() throws Exception {
        FlushAdditionalRedshiftRegisterTask taskSpy = spy(new FlushAdditionalRedshiftRegisterTask());
        doReturn(mockConnection).when(taskSpy).getConnection(config);
        taskSpy.run(storageDataFileKeys,config);
        verify(mockStatement, times(1)).execute("INSERT INTO event_files_merge_manager VALUES ('topic1','path/to/file1.gz','UPLOADED',getdate())");
        verify(mockStatement, times(1)).execute("INSERT INTO event_files_merge_manager VALUES ('topic1','path/to/file/number/2.gz','UPLOADED',getdate())");
        verify(mockStatement, times(1)).execute("INSERT INTO event_files_merge_manager VALUES ('topic1','path/to/file/number/3.gz','UPLOADED',getdate())");
        verify(mockStatement, times(1)).execute("INSERT INTO event_files_merge_manager VALUES ('topic2','topic2/path/to/file1.gz','UPLOADED',getdate())");
        verify(mockStatement, times(1)).execute("INSERT INTO event_files_merge_manager VALUES ('topic2','topic2/path/to/file/number/2.gz','UPLOADED',getdate())");
        verify(mockStatement, times(5)).execute(anyString());
        verify(mockConnection, times(1)).commit();
    }
    @Test
    public void testRun2() throws Exception {
        FlushAdditionalSnowflakeRegisterTask taskSpy = new FlushAdditionalSnowflakeRegisterTask();
        taskSpy.run(storageDataFileKeys,config);
    }
}

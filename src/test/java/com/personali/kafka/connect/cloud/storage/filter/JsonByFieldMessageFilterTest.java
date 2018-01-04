package com.personali.kafka.connect.cloud.storage.filter;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by orsher on 1/4/18.
 */

@RunWith(MockitoJUnitRunner.class)
public class JsonByFieldMessageFilterTest {
    JsonByFieldMessageFilter filter;

    @Mock
    SinkRecord record1;

    @Mock
    SinkRecord record2;

    @Mock
    SinkRecord record3;

    @Mock
    SinkRecord record4;

    @Mock
    SinkRecord record5;

    @Mock
    SinkRecord record6;

    @Before
    public void setUp() throws Exception {
        Map<String,String> props = new HashMap<>();
        props.put("filter.out.topics", "topic1, topic2,topic3");
        props.put("filter.out.field.name", "filter_out_field_name");
        props.put("filter.out.field.values", "1, 2,3,filter");
        filter = new JsonByFieldMessageFilter(props);

        //Records to be filtered
        when(record1.value()).thenReturn("{\"filter_out_field_name\":\"1\", \"sub\":{}, \"event_time\":\"2016-01-01 10:00:00\"}");
        when(record2.value()).thenReturn("{\"filter_out_field_name\":\"2\", \"sub\":{}, \"event_time\":\"2016-01-01 10:00:00\"}");
        when(record3.value()).thenReturn("{\"filter_out_field_name\":\"3\", \"sub\":{}, \"event_time\":\"2016-01-01 10:00:00\"}");
        when(record4.value()).thenReturn("{\"sub\":{}, \"event_time\":\"2016-01-01 10:00:00\", \"filter_out_field_name\":\"filter\"}");

        //Records that should not be filtered
        when(record5.value()).thenReturn("{\"sub\":{}, \"event_time\":\"2016-01-01 10:00:00\"}");
        when(record6.value()).thenReturn("{\"filter_out_field_name\":\"filter1\", \"sub\":{}, \"event_time\":\"2016-01-01 10:00:00\"}");


    }

    private void setMocksTopic(String topic){
        when(record1.topic()).thenReturn(topic);
        when(record2.topic()).thenReturn(topic);
        when(record3.topic()).thenReturn(topic);
        when(record4.topic()).thenReturn(topic);
        when(record5.topic()).thenReturn(topic);
        when(record6.topic()).thenReturn(topic);
    }


    @Test
    public void shouldFilterOutConfigureddTopics() throws Exception {
        for (String topic : Arrays.asList("topic1", "topic2", "topic3")) {
            setMocksTopic(topic);
            assertEquals(filter.shouldFilterOut(record1), true);
            assertEquals(filter.shouldFilterOut(record2), true);
            assertEquals(filter.shouldFilterOut(record3), true);
            assertEquals(filter.shouldFilterOut(record4), true);
            assertEquals(filter.shouldFilterOut(record5), false);
            assertEquals(filter.shouldFilterOut(record6), false);
        }
    }

    @Test
    public void shouldFilterOutNonConfigureddTopics() throws Exception {
        for (String topic : Arrays.asList("topic1q", "a")) {
            setMocksTopic(topic);
            assertEquals(filter.shouldFilterOut(record1), false);
            assertEquals(filter.shouldFilterOut(record2), false);
            assertEquals(filter.shouldFilterOut(record3), false);
            assertEquals(filter.shouldFilterOut(record4), false);
            assertEquals(filter.shouldFilterOut(record5), false);
            assertEquals(filter.shouldFilterOut(record6), false);
        }
    }

}
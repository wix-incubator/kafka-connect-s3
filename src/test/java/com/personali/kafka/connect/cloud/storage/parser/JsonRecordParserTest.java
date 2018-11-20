package com.personali.kafka.connect.cloud.storage.parser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Created by or on 28/12/16.
 */
public class JsonRecordParserTest {

    @Test
    public void getStringField() throws Exception {
        JsonRecordParser jrp = new JsonRecordParser();
        String json = "{\"id\":\"1\", \"sub\":{}, \"event_time\":\"2016-01-01 10:00:00\"}";
        String value = jrp.getStringField(json, "id");
        assertThat(value, is("1"));

        json = "{\"id\":1, \"sub\":{}, \"event_time\":\"2016-01-01 10:00:00\"}";
        value = jrp.getStringField(json, "id");
        assertThat(value, is("1"));
    }

    
    @Test
    public void testGetDateFieldSuccess() throws Exception {
        JsonRecordParser jrp = new JsonRecordParser();
        String json = "{\"id\":1, \"sub\":{}, \"event_time\":\"2016-01-01 10:00:00\"}";
        Date event_time = jrp.getDateField(json, "event_time", "yyyy-MM-dd HH:mm:ss",".");
        assertThat(event_time, is((new GregorianCalendar(2016, Calendar.JANUARY, 1, 10,0,0)).getTime()));

        json = "{\"id\":1, \"sub\":{\"sub1\":1}, \"arr1\": [], \"event_time\":\"2016-01-01 10:00:00\"}";
        event_time = jrp.getDateField(json, "event_time", "yyyy-MM-dd HH:mm:ss",".");
        assertThat(event_time, is((new GregorianCalendar(2016, Calendar.JANUARY, 1, 10,0,0)).getTime()));
        
        json = "{\n" + 
        		"	\"timestamp1\": \"2018-11-15T12:19:11.987500\",\n" + 
        		"	\"key1\": {\n" + 
        		"		\"batch\": \"BATCH2457\",\n" + 
        		"		\"offset\": 2457\n" + 
        		"	},\n" + 
        		"	\"DATABASE\": \"moengage\",\n" + 
        		"	\"key2\": {\n" + 
        		"		\"partition\": 7\n" + 
        		"	},\n" + 
        		"	\"EVENTS\": [{\n" + 
        		"		\"action\": \"event0\",\n" + 
        		"		\"timestamp20\": \"2018-11-16T12:19:11\",\n" + 
        		"		\"attributes\": {\n" + 
        		"			\"timestamp20\": \"2018-11-17T12:19:11\"\n" + 
        		"		}\n" + 
        		"	}, {\n" + 
        		"		\"action\": \"event1\",\n" + 
        		"		\"timestamp21\": \"2018-11-18T12:19:11\",\n" + 
        		"		\"attributes\": {\n" + 
        		"			\"timestamp21\": \"2018-11-19T12:19:11\"\n" + 
        		"		}\n" + 
        		"	}, {\n" + 
        		"		\"action\": \"event2\",\n" + 
        		"		\"timestamp22\": \"2018-11-20T12:19:11\",\n" + 
        		"		\"attributes\": {\n" + 
        		"			\"timestamp22\": \"2018-11-21T12:19:11\"\n" + 
        		"		}\n" + 
        		"	}]\n" + 
        		"}";
        event_time = jrp.getDateField(json, "EVENTS.timestamp20", "yyyy-MM-dd'T'HH:mm:ss", ".");
        GregorianCalendar gregorianCalendar = new GregorianCalendar(2018, Calendar.NOVEMBER, 16, 12, 19, 11);
        assertThat(event_time, is((gregorianCalendar).getTime()));
        
        event_time = jrp.getDateField(json, "timestamp1", "yyyy-MM-dd'T'HH:mm:ss", ".");
        gregorianCalendar = new GregorianCalendar(2018, Calendar.NOVEMBER, 15, 12, 19, 11);
        assertThat(event_time, is((gregorianCalendar).getTime()));
        
        
    }
}
package com.deviantart.kafka_connect_s3;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by or on 27/12/16.
 */
public class JsonParserTest {
    @Test
    public void testBasicEventParsing() throws IOException {
        String event = "{}";
        JsonFactory jsonFactory = new JsonFactory();
        JsonParser jp = jsonFactory.createParser(event.getBytes());
        jp.nextToken();
    }
}

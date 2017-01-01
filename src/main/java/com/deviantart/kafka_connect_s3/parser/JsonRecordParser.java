package com.deviantart.kafka_connect_s3.parser;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by or on 28/12/16.
 */
public class JsonRecordParser {

    public Date getDateField(String json, String dateField, String dateFormat) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat(dateFormat);
        JsonFactory f = new JsonFactory();
        Date date = null;

        JsonParser jParser = f.createParser(json.getBytes());
        while (jParser.nextToken() != JsonToken.END_OBJECT) {
            String fieldname = jParser.getCurrentName();
            if (dateField.equals(fieldname)) {
                jParser.nextToken();
                date = df.parse(jParser.getText());
                break;
            }
        }
        jParser.close();

        if ( null == date) {
            throw new Exception("Date Field not found");
        }
        return date;
    }
}

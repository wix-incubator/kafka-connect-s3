package com.personali.kafka.connect.cloud.storage.parser;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
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
            if (jParser.getCurrentToken().isStructStart()) {
               jParser.nextToken();
            }
            String fieldname = jParser.getCurrentName();
            if (dateField.equals(fieldname)) {
                jParser.nextToken();
                date = df.parse(jParser.getText());
                break;
            }
            else{
                jParser.nextToken();
                jParser.skipChildren();
            }
        }
        jParser.close();

        if ( null == date) {
            throw new Exception("Date Field not found");
        }
        return date;
    }

    /**
     * Get a specific field string value from a valid json string representation
     * @param json String representation of a valid json
     * @param stringFieldName The name of the field to extract it's value from the json
     * @return  The value of the field inside the json, null if not found.
     * @throws IOException
     */
    public String getStringField(String json, String stringFieldName) throws IOException {
        JsonFactory f = new JsonFactory();
        JsonParser jParser = f.createParser(json.getBytes());
        String value = null;
        while (jParser.nextToken() != JsonToken.END_OBJECT) {
            if (jParser.getCurrentToken().isStructStart()) {
                jParser.nextToken();
            }
            String fieldName = jParser.getCurrentName();
            if (stringFieldName.equals(fieldName)) {
                jParser.nextToken();
                value = jParser.getText();
                break;
            }
            else{
                jParser.nextToken();
                jParser.skipChildren();
            }
        }
        jParser.close();
        return value;

    }
}

package com.personali.kafka.connect.cloud.storage.parser;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

/**
 * Created by or on 28/12/16.
 */
public class JsonRecordParser {


	public  Date getDateField(String json, String dateField, String dateFormat, String delimiter) throws Exception {
    	SimpleDateFormat df = new SimpleDateFormat(dateFormat);
        JsonFactory f = new JsonFactory();
        JsonParser jParser = f.createParser(json.getBytes());
        Date date = null;
        
        
        StringTokenizer searchFields = new StringTokenizer(dateField,delimiter);
        
        
        
        String value = null;
        
        String currentSearchField = searchFields.nextToken();
        
        while (jParser.nextToken() != JsonToken.END_OBJECT ) {
        	
            while (jParser.getCurrentToken().isStructStart() || 
            		jParser.getCurrentToken() == JsonToken.START_ARRAY || 
            		jParser.getCurrentToken() == JsonToken.END_ARRAY) {
                jParser.nextToken();
            }
            String fieldName = jParser.getCurrentName();

            if (fieldName.equals(currentSearchField)) {
                jParser.nextToken();
                if (jParser.getCurrentToken() == JsonToken.START_ARRAY) {
                	jParser.nextToken();
                }
                else {
	                value = jParser.getText();
                }
                
                if (searchFields.hasMoreTokens() == false) {
                	break;
                }
                else {
                	currentSearchField = searchFields.nextToken();
                }
                	
            }
            else{
                jParser.nextToken();
                jParser.skipChildren();
            }
        }
        jParser.close();
        if(searchFields.hasMoreTokens()) {
        	throw new Exception("Date Field not found");
        }
        else {
        	try {
        		date = df.parse(value);
        	}
        	catch(Exception e) {
        		throw new Exception("Unable to Parse this value "+value+" into date format provided");
        	}
        }
        return date;

    }


    /**
     * Get a specific field string value from a valid json string representation
     * @param json String representation of a valid json
     * @param stringFieldName The name of the field to extract it's value from the json
     * @return  The value of the field inside the json, null if not found.
     * @throws JsonParseException 
     * @throws IOException
     */
    public String getStringField(String json, String stringFieldName, String delimiter) throws JsonParseException, IOException {
    	
        JsonFactory f = new JsonFactory();
        JsonParser jParser = f.createParser(json.getBytes());
        
        StringTokenizer searchFields = new StringTokenizer(stringFieldName,delimiter);
        
        String value = null;
        
        String currentSearchField = searchFields.nextToken();
        
        while (jParser.nextToken() != JsonToken.END_OBJECT ) {
        	
            while (jParser.getCurrentToken().isStructStart() || 
            		jParser.getCurrentToken() == JsonToken.START_ARRAY || 
            		jParser.getCurrentToken() == JsonToken.END_ARRAY) {
                jParser.nextToken();
            }
            String fieldName = jParser.getCurrentName();

            if (fieldName.equals(currentSearchField)) {
                jParser.nextToken();
                if (jParser.getCurrentToken() == JsonToken.START_ARRAY) {
                	jParser.nextToken();
                }
                else {
	                value = jParser.getText();
                }
                
                if (searchFields.hasMoreTokens() == false) {
                	break;
                }
                else {
                	currentSearchField = searchFields.nextToken();
                }
                	
            }
            else{
                jParser.nextToken();
                jParser.skipChildren();
            }
        }
        jParser.close();
        if(searchFields.hasMoreTokens()) {
        	value = null;
        }
       
        return value;

    }
}

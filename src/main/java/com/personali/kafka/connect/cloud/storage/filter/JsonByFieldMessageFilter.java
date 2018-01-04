package com.personali.kafka.connect.cloud.storage.filter;

import com.personali.kafka.connect.cloud.storage.parser.JsonRecordParser;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by orsher on 1/3/18.
 */
public class JsonByFieldMessageFilter extends MessageFilter {
    private static final Logger log = LoggerFactory.getLogger(JsonByFieldMessageFilter.class);

    private List<String> filterTopics;
    private String filterFieldName;
    private List<String> filterFieldValues;
    private JsonRecordParser jsonParser = new JsonRecordParser();

    public JsonByFieldMessageFilter(Map<String, String> props){
        super(props);
        String topics = props.get("filter.out.topics");
        if (topics != null){
            filterTopics = Arrays.stream(topics.split(",")).map(String::trim).collect(Collectors.toList());
        }
        filterFieldName = props.get("filter.out.field.name");
        String fieldValues = props.get("filter.out.field.values");
        if (fieldValues != null){
            filterFieldValues = Arrays.stream(fieldValues.split(",")).map(String::trim).collect(Collectors.toList());
        }
        if (filterFieldName == null || filterFieldValues == null || filterTopics == null){
            throw new ConnectException("JsonByFieldMessageFilter properties were not found");
        }
    }

    /**
     * Check if Json kafka record should be filtered out using the config
     *
     * @param record A Kafka SinkRecord to check if it should be filtered out of not
     * @return true if record should be filtered out, false otherwise
     */
    @Override
    public boolean shouldFilterOut(SinkRecord record) {
        try {
            //First check if this record topic configured to be filtered
            if (filterTopics.contains(record.topic())) {

                //Get the field value from the json
                String fieldValue = jsonParser.getStringField((String) record.value(), this.filterFieldName);

                //If the field exists and the value should be filtered return true
                if (fieldValue != null && filterFieldValues.contains(fieldValue)) {
                    return true;
                } else {
                    return false;
                }
            }
            else{
                return false;
            }
        } catch (IOException e) {
            log.error("Could not parse message for filtering. Not filtering out field. message: {}",record.value(),e);
            return false;
        }
    }
}

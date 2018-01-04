package com.personali.kafka.connect.cloud.storage.filter;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * Created by orsher on 1/3/18.
 */
public abstract class MessageFilter {
    private Map<String,String> props;

    public MessageFilter(Map<String,String> props){
        this.props = props;
    }

    abstract public boolean shouldFilterOut(SinkRecord record);

}

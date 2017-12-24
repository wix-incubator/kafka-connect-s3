package com.personali.kafka.connect.cloud.storage.flush;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by orsher on 1/3/17.
 */
public interface FlushAdditionalTask {
    void run(Map<TopicPartition,ArrayList<String>> s3DataFileKeys, Map<String,String> config);
}

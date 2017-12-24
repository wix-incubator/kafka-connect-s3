package com.personali.kafka.connect.cloud.storage.flush;

import com.google.cloud.bigquery.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;

/**
 * Created by orsher on 1/3/17.
 */
public class FlushAdditionalBigQueryRegisterTask implements FlushAdditionalTask {

    private static final Logger log = LoggerFactory.getLogger(FlushAdditionalBigQueryRegisterTask.class);

    private final static String BASE_INSERT_TEMPLATE =  "INSERT INTO `%s` (topic,file,status,create_time) VALUES ";
    private final static String ROW_INSERT_TEMPLATE =  "('%s','%s','UPLOADED',current_timestamp())";
    private StringJoiner statementJoiner;

    public void run(Map<TopicPartition,ArrayList<String>> storageDataFileKeys, Map<String,String> config) throws ConnectException{
        //Create BigQuery client
        BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

        //Initialize insert statement
        statementJoiner = new StringJoiner(",",String.format(BASE_INSERT_TEMPLATE,config.get("bigquery.table.name")),"");

        //For each file, if topic is configured to be registered, do so
        for (Map.Entry<TopicPartition,ArrayList<String>> entry : storageDataFileKeys.entrySet()){
            if (shouldRegisterTopicFiles(entry.getKey().topic(),config)) {
                for (String storageDataFileKey : entry.getValue()) {
                    addFileToInsertStatement(entry.getKey().topic(), storageDataFileKey);
                }
            }
        }

        runQuery(bigQuery, statementJoiner.toString());
    }

    private boolean shouldRegisterTopicFiles(String topic, Map<String, String> config) {
        if (config.containsKey("bigquery.register."+topic) && config.get("bigquery.register."+topic).equals("true")){
            return true;
        }
        else{
            return false;
        }
    }

    private void addFileToInsertStatement(String topic, String storageDataFileKey) {
        statementJoiner.add(String.format(ROW_INSERT_TEMPLATE,topic,storageDataFileKey));
    }

    private void runQuery(BigQuery bigQuery, String query){
        log.info("Running the following sql: {}",query);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query)
                        .setUseLegacySql(false)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());

        //Create the Job
        Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        try {
            queryJob = queryJob.waitFor();
        } catch (InterruptedException e) {
            log.error("Failed to register storage data file to BigQuery: ",e);
            throw new RuntimeException("Failed to register storage data file to BigQuery: ",e);
        }

        // Check for errors
        if (queryJob == null) {
            log.error("Failed to register storage data file to BigQuery: Job no longer exists");
            throw new RuntimeException("Failed to register storage data file to BigQuery: Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            log.error("Failed to register storage data file to BigQuery: {}",queryJob.getStatus().getError().toString());
            throw new RuntimeException("Failed to register storage data file to BigQuery: " + queryJob.getStatus().getError().toString());
        }

    }


}

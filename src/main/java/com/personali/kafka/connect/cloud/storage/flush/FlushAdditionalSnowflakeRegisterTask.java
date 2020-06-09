package com.personali.kafka.connect.cloud.storage.flush;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;


public class FlushAdditionalSnowflakeRegisterTask implements FlushAdditionalTask {

    private static final Logger log = LoggerFactory.getLogger(FlushAdditionalMysqlRegisterTask.class);

    private final static String BASE_INSERT_TEMPLATE =  "INSERT INTO %s (topic,file,status,create_time) VALUES ";
    private final static String ROW_INSERT_TEMPLATE =  "('%s','%s','UPLOADED','2020-06-08')";
    private StringJoiner statementJoiner;

    public void run(Map<TopicPartition,ArrayList<String>> storageDataFileKeys, Map<String,String> config) throws ConnectException{
        Connection snowflakeConnection;
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");

// replace "<password>" with your password
            Properties props = new Properties();
            props.put("user", config.get("snowflake.user"));
            props.put("password", config.get("snowflake.password"));
            props.put("db",config.get("snowflake.db"));

            props.put("schema", config.get("snowflake.schema"));

            snowflakeConnection = DriverManager.getConnection(config.get("snowflake.jdbc.url"), props);
            if (snowflakeConnection != null) {
                log.info("snowflake Connection Successful!");
            } else {
                log.error("Failed to make connection!");
            }
        } catch (SQLException e) {
            log.error("snowflake Connection Failed!",e);
            throw new ConnectException(e);
        } catch (ClassNotFoundException e) {
            log.error("Couldn't find snowflake jdbc driver",e);
            throw new ConnectException(e);
        }


        //Initialize insert statement
        statementJoiner = new StringJoiner(",",String.format(BASE_INSERT_TEMPLATE,config.get("snowflake.table.name")),"");

        boolean anyFileToRegister = false;
        //For each file, if topic is configured to be registered, do so
        for (Map.Entry<TopicPartition,ArrayList<String>> entry : storageDataFileKeys.entrySet()){
            log.info("Handling files for topic {}",entry.getKey().topic());
            if (shouldRegisterTopicFiles(entry.getKey().topic(),config)) {
                log.info("Registering file");
                for (String storageDataFileKey : entry.getValue()) {
                    log.info("Registering file {}", storageDataFileKey);
                    addFileToInsertStatement(entry.getKey().topic(), storageDataFileKey);
                    anyFileToRegister=true;
                }
            }
        }

        if (anyFileToRegister) {
            runQuery(snowflakeConnection, statementJoiner.toString());
        }
    }

    private boolean shouldRegisterTopicFiles(String topic, Map<String, String> config) {
        if (config.containsKey("snowflake.register."+topic) && config.get("snowflake.register."+topic).equals("true")){
            return true;
        }
        else{
            return false;
        }
    }

    private void addFileToInsertStatement(String topic, String storageDataFileKey) {
        statementJoiner.add(String.format(ROW_INSERT_TEMPLATE,topic,storageDataFileKey));
    }

    private void runQuery(Connection snowflakeConnection, String query){
        log.info("Running the following sql: {}",query);
        try {
            PreparedStatement insertStatement = snowflakeConnection.prepareStatement(query);
            insertStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                snowflakeConnection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }


}

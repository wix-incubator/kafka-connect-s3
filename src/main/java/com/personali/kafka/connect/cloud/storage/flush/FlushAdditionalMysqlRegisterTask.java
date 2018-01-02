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
import java.util.StringJoiner;

/**
 * Created by orsher on 1/3/17.
 */
public class FlushAdditionalMysqlRegisterTask implements FlushAdditionalTask {

    private static final Logger log = LoggerFactory.getLogger(FlushAdditionalMysqlRegisterTask.class);

    private final static String BASE_INSERT_TEMPLATE =  "INSERT INTO %s (topic,file,status,create_time) VALUES ";
    private final static String ROW_INSERT_TEMPLATE =  "('%s','%s','UPLOADED',now())";
    private StringJoiner statementJoiner;

    public void run(Map<TopicPartition,ArrayList<String>> storageDataFileKeys, Map<String,String> config) throws ConnectException{
        Connection mysqlConnection;
        try {
            Class.forName("com.mysql.jdbc.Driver");
			mysqlConnection = DriverManager.getConnection(config.get("mysql.jdbc.url"),config.get("mysql.user"), config.get("mysql.password"));
			if (mysqlConnection != null) {
				log.info("Mysql Connection Successful!");
			} else {
				log.error("Failed to make connection!");
			}
		} catch (SQLException e) {
			log.error("MySQL Connection Failed!",e);
            throw new ConnectException(e);
		} catch (ClassNotFoundException e) {
            log.error("Couldn't find mysql jdbc driver",e);
            throw new ConnectException(e);
        }


        //Initialize insert statement
        statementJoiner = new StringJoiner(",",String.format(BASE_INSERT_TEMPLATE,config.get("mysql.table.name")),"");

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
            runQuery(mysqlConnection, statementJoiner.toString());
        }
    }

    private boolean shouldRegisterTopicFiles(String topic, Map<String, String> config) {
        if (config.containsKey("mysql.register."+topic) && config.get("mysql.register."+topic).equals("true")){
            return true;
        }
        else{
            return false;
        }
    }

    private void addFileToInsertStatement(String topic, String storageDataFileKey) {
        statementJoiner.add(String.format(ROW_INSERT_TEMPLATE,topic,storageDataFileKey));
    }

    private void runQuery(Connection mysqlConnection, String query){
        log.info("Running the following sql: {}",query);
        try {
            PreparedStatement insertStatement = mysqlConnection.prepareStatement(query);
            insertStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                mysqlConnection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }


}

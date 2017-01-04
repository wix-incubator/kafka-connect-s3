package com.deviantart.kafka_connect_s3.flush;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/**
 * Created by orsher on 1/3/17.
 */
public class FlushAdditionalRedshiftRegisterTask implements FlushAdditionalTask {

    private static final Logger log = LoggerFactory.getLogger(FlushAdditionalRedshiftRegisterTask.class);

    private final static String INSERT_TEMPLATE =  "INSERT INTO %s VALUES ('%s','%s','UPLOADED',getdate())";

    public void run(Map<TopicPartition,ArrayList<String>> s3DataFileKeys, Map<String,String> config) throws ConnectException{
        try {
            Class.forName("com.amazon.redshift.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new ConnectException("Redshift JDBC Driver was not found in class path.");
        }
        try (Connection connection = getConnection(config)){
            for (Map.Entry<TopicPartition,ArrayList<String>> entry : s3DataFileKeys.entrySet()){
                if (shouldRegisterTopicFiles(entry.getKey().topic(),config)) {
                    for (String s3DataFileKey : entry.getValue()) {
                        registerDataFileToRedshfit(connection, config.get("redshift.table.name"), entry.getKey().topic(), s3DataFileKey);
                    }
                }
            }
            connection.commit();
        } catch (SQLException e) {
            log.error("Failed to register files in Redshift.",e);
            throw new ConnectException("Failed to register files in Redshift.");
        }
    }

    private boolean shouldRegisterTopicFiles(String topic, Map<String, String> config) {
        if (config.containsKey("redshift.register."+topic) && config.get("redshift.register."+topic).equals("true")){
            return true;
        }
        else{
            return false;
        }
    }

    private void registerDataFileToRedshfit(Connection connection, String tableName, String topic,  String s3DataFileKey) throws SQLException {
        String stmtStr = String.format(INSERT_TEMPLATE,tableName,topic,s3DataFileKey);
        try (Statement stmt = connection.createStatement()) {

            stmt.execute(stmtStr);

        } catch (SQLException e) {
            log.error("Failed to register s3 data file to Redshift: {} ",stmtStr, e);
            throw e;
        }
    }

    protected Connection getConnection(Map<String,String> config) throws SQLException {
        Properties connProps = new Properties();
        connProps.setProperty("user",config.get("redshift.user"));
        connProps.setProperty("password",config.get("redshift.password"));
        Connection conn = DriverManager.getConnection(config.get("redshift.connection.url"),connProps);
        conn.setAutoCommit(false);
        return conn;
    }

}

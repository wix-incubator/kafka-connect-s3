package com.deviantart.kafka_connect_s3.partition;


import com.deviantart.kafka_connect_s3.BlockGZIPFileWriter;
import com.deviantart.kafka_connect_s3.S3Writer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by orsher on 1/1/17.
 */
public class TopicPartitionFiles {
    private static final Logger log = LoggerFactory.getLogger(TopicPartitionFiles.class);
    private Map<S3Partition,BlockGZIPFileWriter> tmpFiles;
    private TopicPartition topicPartition;
    private String localBufferDir;
    private long gzipChunkThreshold;
    private Long firstOffset=null;
    private Long lastOffset=null;
    private S3Writer s3;

    public  TopicPartitionFiles(TopicPartition topicPartition, String localBufferDir, long gzipChunkThreshold, S3Writer s3){
        tmpFiles = new HashMap<>();
        this.topicPartition = topicPartition;
        this.localBufferDir = localBufferDir;
        this.gzipChunkThreshold = gzipChunkThreshold;
        this.s3=s3;
    }

    private BlockGZIPFileWriter getFileForTopicPartitionKey(S3Partition tpKey, long currentOffset) throws IOException{
        BlockGZIPFileWriter w = tmpFiles.get(tpKey);
        //Case the file for this partition was not created yet
        if (w == null){
            //Create new file
            w = createNextBlockWriter(tpKey, currentOffset);
            //Add file writer to map
            tmpFiles.put(tpKey,w);
        }
        return w;
    }

    public void revokeFiles(){
        for (Map.Entry<S3Partition, BlockGZIPFileWriter> entry : tmpFiles.entrySet()) {
            log.info("Revoked topic partition key {}, deleting buffer", entry.getKey());
            try {
                entry.getValue().close();
                entry.getValue().delete();
            } catch (IOException ioe) {
                throw new ConnectException("Failed to resume TopicPartition form S3", ioe);
            }
        }
    }

    public void putRecord(S3Partition tpKey, long recordOffset, String recordValue) {
        try {
            BlockGZIPFileWriter buffer = getFileForTopicPartitionKey(tpKey,recordOffset);
            buffer.write(recordValue);
        } catch (IOException e) {
            throw new RetriableException("Failed to write to buffer", e);
        }
        if (firstOffset == null){
            firstOffset=recordOffset;
        }
        lastOffset=recordOffset;
    }

    public ArrayList flushFiles(){
        boolean anyFileWereUploaded=false;
        ArrayList<String> s3DataFileKeys = new ArrayList<>();
        try {
            for (Map.Entry<S3Partition, BlockGZIPFileWriter> entry : tmpFiles.entrySet()) {
                BlockGZIPFileWriter writer = entry.getValue();
                S3Partition s3Partition = entry.getKey();
                if (writer.getNumRecords() == 0) {
                    // Not done anything yet
                    log.info("No new records for topic partition key: {}", s3Partition);
                    continue;
                }
                writer.close();
                String s3DataFileKey = s3.putChunk(writer.getDataFilePath(), writer.getIndexFilePath(), s3Partition);
                s3DataFileKeys.add(s3DataFileKey);
                anyFileWereUploaded=true;
                log.info("Successfully uploaded chunk for {}", s3Partition);
            }
            if (anyFileWereUploaded) {
                s3.updateCursorFile(topicPartition, lastOffset + 1);
                log.info("Successfully uploaded files topic partition. first offset: {} last offset: {}", firstOffset, lastOffset );

                // Now reset files for that topic partition key
                tmpFiles.clear();
                firstOffset=null;
                lastOffset=null;
            }
        } catch (FileNotFoundException fnf) {
            //TODO: delete any file that might have already uploaded to S3
            throw new ConnectException("Failed to find local dir for temp files", fnf);
        } catch (IOException e) {
            throw new RetriableException("Failed S3 upload", e);
        }
        return s3DataFileKeys;
    }

    private BlockGZIPFileWriter createNextBlockWriter(S3Partition tpKey, long nextOffset) throws ConnectException, IOException {
        String fileName = tpKey.getDataFileName();
        String directory = tpKey.getDataDirectory();
        if (localBufferDir == null) {
            throw new ConnectException("No local buffer file path configured");
        }
        return new BlockGZIPFileWriter(fileName, localBufferDir+"/"+directory, nextOffset, gzipChunkThreshold);
    }


}

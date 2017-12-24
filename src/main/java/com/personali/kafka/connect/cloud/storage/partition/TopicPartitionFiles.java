package com.personali.kafka.connect.cloud.storage.partition;


import com.personali.kafka.connect.cloud.storage.BlockGZIPFileWriter;
import com.personali.kafka.connect.cloud.storage.storage.StorageWriter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by orsher on 1/1/17.
 */
public class TopicPartitionFiles {
    private static final Logger log = LoggerFactory.getLogger(TopicPartitionFiles.class);
    private Map<StoragePartition,BlockGZIPFileWriter> tmpFiles;
    private TopicPartition topicPartition;
    private String localBufferDir;
    private long gzipChunkThreshold;
    private Long firstOffset=null;
    private Long lastOffset=null;
    private StorageWriter storageWriter;

    public  TopicPartitionFiles(TopicPartition topicPartition, String localBufferDir, long gzipChunkThreshold, StorageWriter storageWriter){
        tmpFiles = new HashMap<>();
        this.topicPartition = topicPartition;
        this.localBufferDir = localBufferDir;
        this.gzipChunkThreshold = gzipChunkThreshold;
        this.storageWriter =storageWriter;
    }

    private BlockGZIPFileWriter getFileForTopicPartitionKey(StoragePartition tpKey, long currentOffset) throws IOException{
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
        for (Map.Entry<StoragePartition, BlockGZIPFileWriter> entry : tmpFiles.entrySet()) {
            log.info("Revoked topic partition key {}, deleting buffer", entry.getKey());
            try {
                entry.getValue().close();
                entry.getValue().delete();
            } catch (IOException ioe) {
                throw new ConnectException("Failed to resume TopicPartition from storage", ioe);
            }
        }
    }

    public void putRecord(StoragePartition tpKey, long recordOffset, String recordValue) {
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
        ArrayList<String> storageDataFileKeys = new ArrayList<>();
        try {
            for (Iterator<Map.Entry<StoragePartition, BlockGZIPFileWriter>> it = tmpFiles.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<StoragePartition, BlockGZIPFileWriter> entry = it.next();
                BlockGZIPFileWriter writer = entry.getValue();
                StoragePartition storagePartition = entry.getKey();
                if (writer.getNumRecords() == 0) {
                    // Not done anything yet
                    log.info("No new records for topic partition key: {}", storagePartition);
                    continue;
                }
                //Close file and remove from map
                writer.close();
                it.remove();

                //upload to Storage
                String storageDataFileKey = storageWriter.putChunk(writer.getDataFilePath(), writer.getIndexFilePath(), storagePartition);

                //Remove files from local file system
                writer.delete();

                //Add to returned keys
                storageDataFileKeys.add(storageDataFileKey);

                anyFileWereUploaded=true;
                log.info("Successfully uploaded chunk for {}", storagePartition);
            }
            if (anyFileWereUploaded) {
                storageWriter.updateCursorFile(topicPartition, lastOffset + 1);
                log.info("Successfully uploaded files topic partition. first offset: {} last offset: {}", firstOffset, lastOffset );

                firstOffset=null;
                lastOffset=null;
            }
        } catch (FileNotFoundException fnf) {
            //TODO: delete any file that might have already uploaded to Storage
            throw new ConnectException("Failed to find local dir for temp files", fnf);
        } catch (IOException e) {
            throw new RetriableException("Failed storage upload", e);
        }
        return storageDataFileKeys;
    }

    private BlockGZIPFileWriter createNextBlockWriter(StoragePartition tpKey, long nextOffset) throws ConnectException, IOException {
        String fileName = tpKey.getDataFileName();
        String directory = tpKey.getDataDirectory();
        if (localBufferDir == null) {
            throw new ConnectException("No local buffer file path configured");
        }
        return new BlockGZIPFileWriter(fileName, localBufferDir+"/"+directory, nextOffset, gzipChunkThreshold);
    }


}

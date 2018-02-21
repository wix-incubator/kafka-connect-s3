package com.personali.kafka.connect.cloud.storage;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import static org.junit.Assert.assertEquals;

public class ConnectorIT {

    private static final Logger log = LoggerFactory.getLogger(ConnectorIT.class);
    private static final String TEST_TOPIC_NAME = "test-topic";
    private static final String BUCKET_NAME = "fakes3";
    private static final String FIRST_DATE_FORMATTED = "2017/11/11";
    private static final String SECOND_DATE_FORMATTED = "2017/11/12";
    private static final String BUCKET_PREFIX = "connect-system-test/";
    private static final String FILE_PREFIX = "systest/";
    private static final String FILE_PREFIX_WITH_FIRST_DATE = FILE_PREFIX + FIRST_DATE_FORMATTED + "/";
    private static final String INDEXES_FILE_PREFIX_WITH_FIRST_DATE = FILE_PREFIX +"indexes/" + FIRST_DATE_FORMATTED + "/";
    private static final String FILE_PREFIX_WITH_SECOND_DATE = FILE_PREFIX + SECOND_DATE_FORMATTED + "/";
    private static final String INDEXES_FILE_PREFIX_WITH_SECOND_DATE = FILE_PREFIX +"indexes/" + SECOND_DATE_FORMATTED + "/";

    private static KafkaProducer<Integer, String> producer0;
    private static KafkaProducer<Integer, String> producer1;
    private static KafkaProducer<Integer, String> producer2;
    private static KafkaProducer<Integer, String> producer3;
    private static List<ProducerRecord<Integer, String>> messages;
    private static List<String> expectedMessagesInS3PerPartitionFirstDate = Arrays.asList("", "", "");
    private static List<String> expectedMessagesInS3PerPartitionSecondDate = Arrays.asList("", "", "");
    private static AmazonS3Client s3Client;

    @BeforeClass
    public static void oneTimeSetUp() throws InterruptedException {

        String fakeS3Endpoint = "http://localhost:4569";

        BasicAWSCredentials credentials = new BasicAWSCredentials("foo", "bar");
        s3Client = new AmazonS3Client(credentials);
        s3Client.setEndpoint(fakeS3Endpoint);
        s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        prepareMessages();
        initTest();
    }

    private static void prepareMessages(){
        messages = new ArrayList<>(100);

        for (int i = 200; i < 300; i++) {
            int partition = i % 3;
            String message = "{\"event_time\": \"2017-11-11 11:11:11\", \"counter\":" + i + "}";

            String existingMessagesInS3PerPartition = expectedMessagesInS3PerPartitionFirstDate.get(partition);
            existingMessagesInS3PerPartition += message + "\n";
            expectedMessagesInS3PerPartitionFirstDate.set(partition, existingMessagesInS3PerPartition);

            messages.add(
                    new ProducerRecord<>(TEST_TOPIC_NAME, partition, i, message)
            );
        }

        //Create messages for another S3 partition
        for (int i = 200; i < 300; i++) {
            int partition = i % 3;
            String message = "{\"event_time\": \"2017-11-12 11:11:11\", \"counter\":" + i + "}";

            String existingMessagesInS3PerPartition = expectedMessagesInS3PerPartitionSecondDate.get(partition);
            existingMessagesInS3PerPartition += message + "\n";
            expectedMessagesInS3PerPartitionSecondDate.set(partition, existingMessagesInS3PerPartition);

            messages.add(
                    new ProducerRecord<>(TEST_TOPIC_NAME, partition, i, message)
            );
        }

        //Create messages for be filtered
        for (int i = 300; i < 303; i++) {
            int partition = i % 3;
            String message = "{\"event_time\": \"2017-11-12 11:11:11\", \"counter\":" + i + ", \"should_filter\":\"yes\" }";
            messages.add(
                    new ProducerRecord<>(TEST_TOPIC_NAME, partition, i, message)
            );
        }
    }

    private static void initTest() throws InterruptedException {
        String kafkaBrokers = "broker:9092";
        Properties producer0Properties = new Properties();
        producer0Properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        producer0Properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producer0Properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer0Properties.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        producer0Properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producer0Properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-id-1");

        producer0 = new KafkaProducer<>(producer0Properties);


        Properties producer1Properties = new Properties();
        producer1Properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        producer1Properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producer1Properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer1Properties.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        producer1Properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producer1Properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-id-2");

        producer1 = new KafkaProducer<>(producer1Properties);
        producer2 = new KafkaProducer<>(producer1Properties);

        Properties producer2Properties = new Properties();
        producer2Properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        producer2Properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producer2Properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer2Properties.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        producer2Properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producer2Properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-id-3");
        producer3 = new KafkaProducer<>(producer2Properties);



        //Send one message using producer 0 (which have different transactional id than the others)
        //Do not commit the transaction
        producer0.initTransactions();
        producer0.beginTransaction();
        producer0.send(messages.get(0));

        //Send all messages using producer 1 (which have the same transactionl id as producer 2)
        //Do not commit the transaction
        //This will simulate a producer that had failed
        producer1.initTransactions();
        producer1.beginTransaction();
        Iterator<ProducerRecord<Integer, String>> messagesIter = messages.iterator();
        while (messagesIter.hasNext()) {
            producer1.send(messagesIter.next());
        }

        //Send all messages using producer 2 & 3
        //producer2 have the same transactional id as producer 1
        //producer3 have a new transactional id
        //Commit the transaction
        //This will simulate a producer that had recovered
        producer2.initTransactions();
        producer3.initTransactions();
        producer2.beginTransaction();
        producer3.beginTransaction();

        //Use a different producer every 3 messages in order to preserve the previous
        //round robin producing between the 3 partitions
        messagesIter = messages.iterator();
        int counter =0;
        while (messagesIter.hasNext()) {
            if ((counter/3) % 2 == 0)
                producer2.send(messagesIter.next());
            else
                producer3.send(messagesIter.next());
            counter++;
        }
        producer2.commitTransaction();
        producer3.commitTransaction();

        //Abort transactional-id1 transaction or else we would not be able to read from the partitions
        //relevant to this transaction
        producer0.abortTransaction();
        Thread.sleep(60_000L);
    }

    @Test
    public void testTotalNumberOfMessages() throws IOException {
        assertS3FilesNumberOfMessages(getFilesWithPrefixAndSuffix(BUCKET_NAME,BUCKET_PREFIX,".gz"),200);
    }

    @Test
    public void testFirstDatePartition0Files() throws IOException {
        assertS3FileContents(
                BUCKET_PREFIX + FILE_PREFIX + "last_chunk_index.test-topic-00000.txt",
                Arrays.asList("135","134"),
                false
        );

        assertS3IndexFile(
                BUCKET_PREFIX + INDEXES_FILE_PREFIX_WITH_FIRST_DATE + "test-topic-00000-000000000068.index.json",33
        );

        assertS3JsonFileCountByKey(
                BUCKET_PREFIX + FILE_PREFIX_WITH_FIRST_DATE + "test-topic-00000-000000000068.gz",
                expectedMessagesInS3PerPartitionFirstDate.get(0),
                true,
                "counter"
        );
    }

    @Test
    public void testFirstDatePartition1Files() throws IOException {
        assertS3FileContents(
                BUCKET_PREFIX + FILE_PREFIX + "last_chunk_index.test-topic-00001.txt",
                Arrays.asList("135","134"),
                false
        );

        assertS3IndexFile(
                BUCKET_PREFIX + INDEXES_FILE_PREFIX_WITH_FIRST_DATE + "test-topic-00001-000000000068.index.json", 33
        );

        assertS3JsonFileCountByKey(
                BUCKET_PREFIX + FILE_PREFIX_WITH_FIRST_DATE + "test-topic-00001-000000000068.gz",
                expectedMessagesInS3PerPartitionFirstDate.get(1),
                true,
                "counter"
        );
    }

    @Test
    public void testFirstDatePartition2Files() throws IOException {
        assertS3FileContents(
                BUCKET_PREFIX + FILE_PREFIX + "last_chunk_index.test-topic-00002.txt",
                Arrays.asList("140","139"),
                false
        );

        assertS3IndexFile(
                BUCKET_PREFIX + INDEXES_FILE_PREFIX_WITH_FIRST_DATE + "test-topic-00002-000000000071.index.json",34
        );

        assertS3JsonFileCountByKey(
                BUCKET_PREFIX + FILE_PREFIX_WITH_FIRST_DATE + "test-topic-00002-000000000071.gz",
                expectedMessagesInS3PerPartitionFirstDate.get(2),
                true,
                "counter"
        );

    }

    @Test
    public void testSecondDatePartition0Files() throws IOException {
        assertS3IndexFile(
                getFilesWithPrefixAndSuffix(BUCKET_NAME,BUCKET_PREFIX + INDEXES_FILE_PREFIX_WITH_SECOND_DATE + "test-topic-00000-",".index.json").get(0),
                33
        );

        assertS3JsonFileCountByKey(
                getFilesWithPrefixAndSuffix(BUCKET_NAME,BUCKET_PREFIX + FILE_PREFIX_WITH_SECOND_DATE + "test-topic-00000-",".gz").get(0),
                expectedMessagesInS3PerPartitionSecondDate.get(0),
                true,
                "counter"
        );
    }

    @Test
    public void testSecondDatePartition1Files() throws IOException {

        assertS3IndexFile(
                getFilesWithPrefixAndSuffix(BUCKET_NAME,BUCKET_PREFIX + INDEXES_FILE_PREFIX_WITH_SECOND_DATE + "test-topic-00001-",".index.json").get(0),
                33
        );

        assertS3JsonFileCountByKey(
                getFilesWithPrefixAndSuffix(BUCKET_NAME,BUCKET_PREFIX + FILE_PREFIX_WITH_SECOND_DATE + "test-topic-00001-",".gz").get(0),
                expectedMessagesInS3PerPartitionSecondDate.get(1),
                true,
                "counter"
        );
    }

    @Test
    public void testSecondDatePartition2Files() throws IOException {
        assertS3IndexFile(
                getFilesWithPrefixAndSuffix(BUCKET_NAME,BUCKET_PREFIX + INDEXES_FILE_PREFIX_WITH_SECOND_DATE + "test-topic-00002-",".index.json").get(0),
                34
        );

        assertS3JsonFileCountByKey(
                getFilesWithPrefixAndSuffix(BUCKET_NAME,BUCKET_PREFIX + FILE_PREFIX_WITH_SECOND_DATE + "test-topic-00002-",".gz").get(0),
                expectedMessagesInS3PerPartitionSecondDate.get(2),
                true,
                "counter"
        );

    }

    private void assertS3FilesNumberOfMessages(List<String> keys, long totalNumberOfExpectedMessages) throws IOException {
        long numberOfTotalMessages = keys.stream()
                .map(key -> getFileContent(key, true).split("\n"))
                .flatMap(Arrays::stream)
                .count();
        System.out.println("");
        log.info("Number of total messages: {} , expected {}", numberOfTotalMessages, totalNumberOfExpectedMessages);
        assertEquals(totalNumberOfExpectedMessages,numberOfTotalMessages);
    }

    private List<String> getFilesWithPrefixAndSuffix(String bucketName, String prefix, String suffix){
        List<String> keys = s3Client.listObjects(bucketName,prefix)
                .getObjectSummaries()
                .stream()
                .map(object -> object.getKey())
                .filter(key -> key.endsWith(suffix))
                .collect(Collectors.toList());
        log.info("Files in {} / {} : {}",bucketName,prefix,keys);
        return keys;
    }


    private String getFileContent(String key, boolean gzipped) {
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(BUCKET_NAME, key));
        InputStream objectInputStream = s3Object.getObjectContent();
        String objectContent=null;

        try {
            if (gzipped) {
                byte[] res = decompressGzipContent(objectInputStream);
                objectContent = new String(res);
            } else {
                    objectContent = IOUtils.toString(objectInputStream);
            }
            s3Object.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return objectContent;
    }

    private JsonNode stringToJson(String jsonStr){
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readTree(jsonStr);
        } catch (IOException e) {
            log.error("Could not convert the following the jsonNode: {}", jsonStr);
            System.out.println("Could not convert the following the jsonNode: "+ jsonStr);
            return null;
        }
    }

    private void assertS3JsonFileCountByKey(String key, String content, boolean gzipped, String jsonKey) throws IOException {
        String objectContent = getFileContent(key, gzipped);
        log.info("Content: {}",Arrays.asList(content.split("\n")));
        log.info("Content size: {}",Arrays.asList(content.split("\n")).size());
        Map<Integer, Long> expectedMap = Arrays.asList(content.split("\n")).stream()
                .map(jsonStr -> stringToJson(jsonStr))
                .collect(Collectors.groupingBy(o -> o.get(jsonKey).asInt(), Collectors.counting()));
        Map<Integer, Long> realMap = Arrays.asList(objectContent.split("\n")).stream()
                .map(jsonStr -> stringToJson(jsonStr))
                .collect(Collectors.groupingBy(o -> o.get(jsonKey).asInt(), Collectors.counting()));
        log.info("Comparing Content");
        log.info("Expected Content: {}", expectedMap);
        log.info("Real Content: {}", realMap);
        assertEquals(expectedMap,realMap);
    }


    private void assertS3IndexFile(String key, Integer numRecords) throws IOException {
        String objectContent = getFileContent(key, false);
        JsonNode indexJson = stringToJson(objectContent);
        log.info("Index file Content: {}",objectContent);
        log.info("Expected numRecords: {}", numRecords);
        log.info("Real numRecords: {}", indexJson.get("chunks").get(0).get("num_records").asInt());
        assertEquals(numRecords.intValue(), indexJson.get("chunks").get(0).get("num_records").asInt());
    }

    private void assertS3FileContents(String key, List<String> possibleOffsets, boolean gzipped) throws IOException {
        String objectContent = getFileContent(key, gzipped);
        log.info("Comparing Content");
        log.info("Expected Content: {}", possibleOffsets);
        log.info("Real Content: {}", objectContent);
        assert(possibleOffsets.contains(objectContent));
    }

    private byte[] decompressGzipContent(InputStream is){
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try{
            IOUtils.copy(new GZIPInputStream(is), out);
        } catch(IOException e){
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }
}

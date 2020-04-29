package com.TwitterElasticSearchProj;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.client.indices.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ElasticSearchConsumer {
    static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static void main(String[] args) {
        // set up ElasticSearch REST client
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

        // Create new ElasticSearch index
        CreateIndexRequest twitterIndexCreate = new CreateIndexRequest("twitter");
        twitterIndexCreate.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );

        // Set up listener for async index request
        ActionListener<CreateIndexResponse> listener =
                new ActionListener<CreateIndexResponse>() {
                    @Override
                    public void onResponse(CreateIndexResponse createIndexResponse) {
                        logger.info("Index creation response: \n" + createIndexResponse.toString());
                        if (createIndexResponse.isAcknowledged() && createIndexResponse.isShardsAcknowledged()){
                            logger.info("Launching the Kafka Twitter consumer");
                            new ElasticSearchConsumer().run();
                        }
                        try{
                            client.close();
                        } catch (Exception e){
                            logger.error("Error when attempting to close the ElasticSearch client.");
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Error when making index creation request to ElasticSearch.");
                        e.printStackTrace();
                        try{
                            client.close();
                        } catch (Exception e2){
                            logger.error("Error when attempting to close the ElasticSearch client.");
                            e2.printStackTrace();
                        }
                    }
                };

        // check if Twitter index already exists
        GetIndexRequest twitterIndexExists = new GetIndexRequest("twitter");
        try{
            if (!client.indices().exists(twitterIndexExists, RequestOptions.DEFAULT)){
                // send async request
                client.indices().createAsync(twitterIndexCreate, RequestOptions.DEFAULT, listener);
            }
            else{
                new ElasticSearchConsumer().run();
            }
        } catch (Exception e){
            logger.error("Error occurred when checking index existence.");
            e.printStackTrace();
            try{
                client.close();
            } catch (Exception e2){
                logger.error("Error when attempting to close the ElasticSearch client.");
                e2.printStackTrace();
            }
        }

        try{
            client.close();
        } catch (Exception e){
            logger.error("Error when attempting to close the ElasticSearch client.");
            e.printStackTrace();
        }
    }

    public ElasticSearchConsumer(){

    }

    public void run(){
        // create consumer configs
        String bootstrapServers = "localhost:9092";
        String groupId = "twitter-group-1";
        String topic = "twitter_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(latch, topic, bootstrapServers, groupId);

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( ()-> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try{
                latch.await();
            } catch (InterruptedException e){
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted");
            e.printStackTrace();
        } finally {
            logger.info("Application is closing.");
        }

    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        private RestHighLevelClient restClient;

        public ConsumerRunnable(CountDownLatch latch, String topic, String bootstrapServers, String groupId){
            this.latch = latch;
            Properties  properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
            this.restClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http"),
                            new HttpHost("localhost", 9201, "http")));
        }

        @Override
        public void run() {
            // poll for new data
            ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    logger.info("ElasticSearch bulk request status: " + bulkResponse.status());
                    if (bulkResponse.hasFailures()){
                        logger.error("Bulk Twitter request to ElasticSearch failed with responses: \n");
                        for (BulkItemResponse bulkItemResponse : bulkResponse) {
                            if (bulkItemResponse.isFailed()) {
                                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                                logger.error(failure.getCause().toString());
                            }
                        }
                    }
                    else{
                        logger.info("Bulk Twitter request to ElasticSearch succeeded: " + bulkResponse.toString());
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Bulk Twitter request to ElasticSearch failed and did not return a response.");
                    e.printStackTrace();
                }
            };
            try{
                while(true){
                    ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));
                    BulkRequest bulkRequest = new BulkRequest();
                    for (ConsumerRecord<String, String> record: records){
                        JSONObject obj = new JSONObject(record.value());
                        Map<String, String> jsonMap = new HashMap<>();
                        if (obj.has("text")){
                            String uniqueID = obj.getString("id_str");
                            jsonMap.put("id", uniqueID);
                            jsonMap.put("created_at", obj.getString("created_at"));
                            jsonMap.put("author_id", ((JSONObject)obj.get("user")).getString("id_str"));
                            jsonMap.put("text", obj.getString("text"));
                            logger.info("Adding tweet to bulk request: " + jsonMap.toString());
                            bulkRequest.add(new IndexRequest("twitter").id(uniqueID).source(jsonMap));
                        }
                    }
                    if (!records.isEmpty()){
                        try{
                            BulkResponse bulkResponse = restClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                            logger.info("ElasticSearch bulk request status: " + bulkResponse.status());
                            if (bulkResponse.hasFailures()){
                                logger.error("Bulk Twitter request to ElasticSearch failed with responses: \n");
                                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                                    if (bulkItemResponse.isFailed()) {
                                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                                        logger.error(failure.getCause().toString());
                                    }
                                }
                            }
                            else{
                                logger.info("Bulk Twitter request to ElasticSearch succeeded: " + bulkResponse.toString());
                            }
                            logger.info("Committing offsets...");
                            consumer.commitSync();
                            try{
                                Thread.sleep(1000);
                            } catch(InterruptedException e){
                                e.printStackTrace();
                            }
                        } catch (Exception e){
                            logger.error("Bulk Twitter request to ElasticSearch failed and did not return a response.");
                            e.printStackTrace();
                        }

                        //restClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, listener);
                    }
                }
            }
            catch (WakeupException e){
                logger.info("Received shutdown signal!");
            }
            finally {
                consumer.close();
                try{
                    restClient.close();
                } catch(Exception e){
                    logger.error("Error occurred while trying to close ElasticSearch Rest Client.");
                    e.printStackTrace();
                } finally{
                    // tell our "main" code we're done with the consumer
                    latch.countDown();
                }
            }

        }

        public void shutdown(){
            // this will interrupt consumer.poll() with an exception (WakeUpException)
            consumer.wakeup();
        }
    }
}

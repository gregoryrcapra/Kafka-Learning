package com.TwitterElasticSearchProj;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TwitterProducer {

    public TwitterProducer(){}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        // Set up Twitter Client
        TwitterClient client = new TwitterClient(TwitterAuth.CONSUMER_KEY, TwitterAuth.CONSUMER_SECRET, TwitterAuth.TOKEN, TwitterAuth.SECRET);
        client.run();

        final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook invoked...shutting down Twitter client and Kafka producer.");
            client.client.stop();
            producer.flush();
            producer.close();
        }));

        // send data - asynchronous
        while (!client.client.isDone()){
            String msg = "";
            try{
                msg = client.msgQueue.take();
            } catch (Exception e){
                logger.error("Encountered an error while taking from the Twitter message queue. ");
                e.printStackTrace();
                client.client.stop();
                producer.flush();
                producer.close();
            }
            logger.info("Sending Twitter message to producer: " + msg);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter_topic", msg);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or exception thrown
                    if (e != null){
                        logger.error("Error while producing.");
                        e.printStackTrace();
                    }
                }
            });
        }

        client.client.stop();
        producer.flush();
        producer.close();
    }
}

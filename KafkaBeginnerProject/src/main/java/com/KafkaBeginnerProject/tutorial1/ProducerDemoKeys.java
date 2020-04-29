package com.KafkaBeginnerProject.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // send data - asynchronous
        for (int i=0; i < 10; i++){
            String topic = "first_topic";
            String value = "hello world " + Integer.toString(i);
            final String key = "id_" + Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or exception thrown
                    if (e != null){
                        logger.error("Error while producing: " + e);
                    }
                    else{
                        logger.info("Received new metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                        logger.info("Key " + key + " went to partition " + recordMetadata.partition());
                        //keys 1/3/6 -> 0
                        //keys 0/8 -> 1
                        //keys 2/4/5/7/9 -> 2
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}

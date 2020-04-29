package com.KafkaStreamsWordCountProj;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // get stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        // map values to lowercase
        KTable<String, Long> wordCounts = wordCountInput.mapValues(val -> val.toLowerCase())
            // flatmap values to split by space
            .flatMapValues(val -> Arrays.asList(val.split(" ")))
            // selectkey to apply a key
            .selectKey((ignoredKey, word) -> word)
            // groupbykey to group keys before agg
            .groupByKey()
            // count occurrences in each group
            .count();

        // write results back to Kafka
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams stream = new KafkaStreams(builder.build(), config);
        stream.start();
        // optional- print topology
        System.out.println(stream.toString());

        // shutdown hook to correctly close streams app
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }
}

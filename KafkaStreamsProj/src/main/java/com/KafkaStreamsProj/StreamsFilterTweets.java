package com.KafkaStreamsProj;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Properties;

public class StreamsFilterTweets {

    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());


        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // build the topology
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_topic");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 1000
        );
        filteredStream.to("important_tweets_topic");

        // start our streams application
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();

    }

    private static Integer extractUserFollowersInTweet(String tweetJson){
        JSONObject obj = new JSONObject(tweetJson);
        try{
            return ((JSONObject)obj.get("user")).getInt("followers_count");
        } catch (JSONException e){
            //e.printStackTrace();
            return 0;
        }
    }
}

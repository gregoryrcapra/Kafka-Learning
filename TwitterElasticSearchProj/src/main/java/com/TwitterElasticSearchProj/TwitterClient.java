package com.TwitterElasticSearchProj;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterClient {

    private String consumerKey;
    private String consumerSecret;
    private String token;
    private String secret;
    public BasicClient client;
    public BlockingQueue<String> msgQueue;
    public BlockingQueue<Event> eventQueue;


    public TwitterClient(String consumerKey, String consumerSecret, String token, String secret){
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.token = token;
        this.secret = secret;
        // Create an appropriately sized blocking queue
        this.msgQueue = new LinkedBlockingQueue<String>(100000);
        this.eventQueue = new LinkedBlockingQueue<Event>(10000);
    }

    public void run() {
        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // add some track terms
        endpoint.trackTerms(Lists.newArrayList("Covid", "CV-19", "Coronavirus", "quarantine"));
        //endpoint.stallWarnings(false);

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

        // Create a new BasicClient. By default gzip is enabled.
        client = new ClientBuilder()
                .name("GregsTwitterClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue)
                .build();

        // Establish a connection
        client.connect();
    }
}

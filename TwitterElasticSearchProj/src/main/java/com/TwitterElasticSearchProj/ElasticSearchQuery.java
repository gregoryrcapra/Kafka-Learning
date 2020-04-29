package com.TwitterElasticSearchProj;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ElasticSearchQuery {

    static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());


    public static void main(String[] args) {

        // set up REST client
        RestHighLevelClient restClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

        // construct search request
        SearchRequest searchRequest = new SearchRequest("twitter");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(0);
        sourceBuilder.size(10000);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);

        // set up async listener
        ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                logger.info("Search request status: " + searchResponse.status().toString());
                SearchHits hits = searchResponse.getHits();
                SearchHit[] searchHits = hits.getHits();
                int totalShards = searchResponse.getTotalShards();
                int successfulShards = searchResponse.getSuccessfulShards();
                int failedShards = searchResponse.getFailedShards();
                logger.info("Total Shards: " + totalShards);
                logger.info("Successful Shards: " + successfulShards);
                logger.info("Failed Shards: " + failedShards);
                int i = 1;
                for (SearchHit hit : searchHits) {
                    // do something with the SearchHit
                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    logger.info("Data, hit " + i++ + ": " + hit.getSourceAsString());
                }

                try{
                    restClient.close();
                } catch (Exception e){
                    logger.error("Error when attempting to close the ElasticSearch client.");
                    e.printStackTrace();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("An error occurred while requesting an index search.");
                e.printStackTrace();
                try{
                    restClient.close();
                } catch (Exception e2){
                    logger.error("Error when attempting to close the ElasticSearch client.");
                    e2.printStackTrace();
                }
            }
        };

        // execute async search request
        restClient.searchAsync(searchRequest, RequestOptions.DEFAULT, listener);
    }
}

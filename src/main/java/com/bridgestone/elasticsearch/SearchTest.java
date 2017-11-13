package com.bridgestone.elasticsearch;

import com.bridgestone.properties.ApplicationProperties;
import org.apache.http.HttpHost;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Created by francesco on 28/10/17.
 */
public class SearchTest {
    public static void main(String[] args){
        /*RestClient lowClient = RestClient.builder(new HttpHost("search-my-elastic-domain-dioeomsyqpdv2m5yzqghk5wqrq.eu-central-1.es.amazonaws.com", 443, "https")).build();
        RestHighLevelClient client = new RestHighLevelClient(lowClient);
        TermQueryBuilder builder = QueryBuilders.termQuery("section", "0.01.0");
        QueryBuilder matchSpecificFieldQuery= QueryBuilders.boolQuery().filter(builder);
        SearchRequest searchRequest = new SearchRequest("streetindex");
        searchRequest.types("streetinfo");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchSpecificFieldQuery);
        searchRequest.source(searchSourceBuilder);

        SimpleQueryStringBuilder builder1 = QueryBuilders.simpleQueryStringQuery("+0.0.2.0-1.0.2.0").
                field("edges").boost(2.0f);
        QueryBuilder matchSpecificFieldQuery1= QueryBuilders.boolQuery().must(builder1);
        SearchRequest searchRequest1 = new SearchRequest("streetindex");
        searchRequest.types("streetinfo");
        SearchSourceBuilder searchSourceBuilder1 = new SearchSourceBuilder();
        searchSourceBuilder1.query(matchSpecificFieldQuery1);
        searchRequest1.source(searchSourceBuilder1);
        try {
            /*SearchResponse searchResponse = client.search(searchRequest);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                String section = (String) sourceAsMap.get("section");
                String speed = (String) sourceAsMap.get("speed");
                String key = (String) sourceAsMap.get("keyStreet");
                String index = hit.getIndex();
                String type = hit.getType();
                String id = hit.getId();
                System.err.println(section + "\t" + speed + "\t" + index + "\t" + type + "\t" + id +
                        "\t" + key);

            }*/

            /** we're showing all the results of the search which are ordered by score: for the query we need
             * we just have to take the first result, not everyone
             */

            /*SearchResponse searchResponse = client.search(searchRequest);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                String section = (String) sourceAsMap.get("section");
                String speed = (String) sourceAsMap.get("speed");
                String edges = (String) sourceAsMap.get("edges");
                String index = hit.getIndex();
                String type = hit.getType();
                String id = hit.getId();
                System.err.println(section + "\t" + speed + "\t" + index + "\t" + type + "\t" + id +
                        "\t" + edges);
            }
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        CloudClient cloudClient= new CloudClient();
        /****String address = ApplicationProperties.getElasticSearchAddress();*/
        String address = "search-my-elastic-domain-dioeomsyqpdv2m5yzqghk5wqrq.eu-central-1.es.amazonaws.com";
        SearchHit[] searchHits = null;
        try {
            searchHits = cloudClient.getSection(address, 443, "streetindex",
                    "streetinfo", "0.01.0");
            for (SearchHit hit : searchHits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                String section = (String) sourceAsMap.get("section");
                String speed = (String) sourceAsMap.get("speed");
                String key = (String) sourceAsMap.get("keyStreet");
                String index = hit.getIndex();
                String type = hit.getType();
                String id = hit.getId();
                System.err.println(section + "\t" + speed + "\t" + index + "\t" + type + "\t" + id +
                        "\t" + key);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.err.println("Seconda search\n\n\n");
        try {
            searchHits = cloudClient.getNodeByEdge(address, 443, "streetindex",
                    "streetinfo", "1.0.2.0-2.0.2.0");
            for (SearchHit hit : searchHits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                String section = (String) sourceAsMap.get("section");
                String speed = (String) sourceAsMap.get("speed");
                String edges = (String) sourceAsMap.get("edges");
                String index = hit.getIndex();
                String type = hit.getType();
                String id = hit.getId();
                System.err.println(section + "\t" + speed + "\t" + index + "\t" + type + "\t" + id +
                        "\t" + edges);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}

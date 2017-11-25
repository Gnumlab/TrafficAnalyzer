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

        CloudClient cloudClient= new CloudClient();
        ApplicationProperties.loadProperties();
        String address = ApplicationProperties.getElasticSearchAddress();
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

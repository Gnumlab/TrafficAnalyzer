package com.bridgestone.ElasticSearch;

import com.bridgestone.utils.ConfigurationProperties;
import com.bridgestone.utils.ElasticClient;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by francesco on 15/09/17.
 */
public class CloudClient implements ElasticClient {
    @Override
    public UpdateResponse updateSpeedStreet(String address, int port, String index, String type, String streetKey, Double speed) throws IOException, ExecutionException, InterruptedException {
        RestClient lowClient = RestClient.builder(new HttpHost(address, 443, "https")).build();
        RestHighLevelClient client = new RestHighLevelClient(lowClient);
        UpdateRequest request = new UpdateRequest("posts", "doc", "1")
                .doc(jsonBuilder()
                        .startObject()
                        .field("speed", speed.toString())
                        .endObject());
        return client.update(request);
    }

    public IndexResponse createIndexes(String address, int port, String index, String type,String edges, String streetKey) throws IOException {
        RestClient lowClient = RestClient.builder(new HttpHost(address, 443, "https")).build();
        RestHighLevelClient client = new RestHighLevelClient(lowClient);
        IndexRequest indexRequest = new IndexRequest("posts", "doc", "1")
                .source(jsonBuilder().startObject()
                .field("edges", streetKey + "]")
                .field("section", ConfigurationProperties.TOPIC)
                .field("speed", "50")
                .field("keyStreet", streetKey)
                .endObject());
        return client.index(indexRequest);
    }
}

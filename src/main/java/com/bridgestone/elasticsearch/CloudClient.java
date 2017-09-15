package com.bridgestone.elasticsearch;

import com.bridgestone.utils.ConfigurationProperties;
import com.bridgestone.utils.ElasticClient;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

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
        UpdateRequest request = new UpdateRequest(index, type, streetKey)
                .doc(jsonBuilder()
                        .startObject()
                        .field("speed", speed.toString())
                        .endObject());
        UpdateResponse response = client.update(request);
        lowClient.close();
        return response;
    }

    public IndexResponse createIndexes(String address, int port, String index, String type,String edges, String streetKey) throws IOException {
        RestClient lowClient = RestClient.builder(new HttpHost(address, 443, "https")).build();
        RestHighLevelClient client = new RestHighLevelClient(lowClient);
        IndexRequest indexRequest = new IndexRequest(index, type, streetKey)
                .source(jsonBuilder().startObject()
                .field("edges", streetKey + "]")
                .field("section", ConfigurationProperties.TOPIC)
                .field("speed", "50")
                .field("keyStreet", streetKey)
                .endObject());
        IndexResponse response = client.index(indexRequest);
        lowClient.close();
        return response;
    }

    public GetResponse get(String address, int port, String index, String type, String id) throws IOException {
        RestClient lowClient = RestClient.builder(new HttpHost(address, 443, "https")).build();
        RestHighLevelClient client = new RestHighLevelClient(lowClient);
        GetResponse response = client.get(new GetRequest(index, type, id));

        lowClient.close();
        return response;
    }
}

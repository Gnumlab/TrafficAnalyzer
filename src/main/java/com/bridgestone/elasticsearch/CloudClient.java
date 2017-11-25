package com.bridgestone.elasticsearch;

import com.bridgestone.utils.ElasticClient;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

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

    public IndexResponse createIndexes(String address, int port, String index, String type,String edges, String topic, String streetKey) throws IOException {
        RestClient lowClient = RestClient.builder(new HttpHost(address, 443, "https")).build();
        RestHighLevelClient client = new RestHighLevelClient(lowClient);
        IndexRequest indexRequest = new IndexRequest(index, type, streetKey)
                .source(jsonBuilder().startObject()
                .field("edges", edges + "]")
                .field("section", topic)
                .field("speed", "50")
                .field("keyStreet", streetKey)
                .endObject());
        IndexResponse response = client.index(indexRequest);
        lowClient.close();
        return response;
    }

    public GetResponse getStreet(String address, int port, String index, String type, String id) throws IOException {
        //query to elaticsearch: get street by id
        RestClient lowClient = RestClient.builder(new HttpHost(address, 443, "https")).build();
        RestHighLevelClient client = new RestHighLevelClient(lowClient);
        GetResponse response = client.get(new GetRequest(index, type, id));

        lowClient.close();
        return response;
    }

    public SearchHit[] getSection(String address, int port, String index, String type, String section) throws IOException {
        //query to elaticsearch: get section by id of section itself (it is also a topic name)
        RestClient lowClient = RestClient.builder(new HttpHost(address, port, "https")).build();
        RestHighLevelClient client = new RestHighLevelClient(lowClient);
        TermQueryBuilder builder = QueryBuilders.termQuery("section", section);
        QueryBuilder matchSpecificFieldQuery= QueryBuilders.boolQuery().filter(builder);
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchSpecificFieldQuery);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        return searchHits;
    }

    public SearchHit[] getNodeByEdge(String address, int port, String index, String type, String edge) throws IOException{
        RestClient lowClient = RestClient.builder(new HttpHost(address, port, "https")).build();
        RestHighLevelClient client = new RestHighLevelClient(lowClient);
        SimpleQueryStringBuilder builder = QueryBuilders.simpleQueryStringQuery("+" + edge).
                field("edges").boost(2.0f);
        QueryBuilder matchSpecificFieldQuery= QueryBuilders.boolQuery().must(builder);
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchSpecificFieldQuery);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        return searchHits;
    }
}

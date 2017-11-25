package com.bridgestone.elasticsearch;

import com.bridgestone.utils.ElasticClient;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by francesco on 15/09/17.
 */
public class LocalClient implements ElasticClient{

    public UpdateResponse updateSpeedStreet(String address, int port, String index,
                                            String type, String streetKey, Double speed)
            throws IOException, ExecutionException, InterruptedException {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),
                        9300));
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id(streetKey);
        updateRequest.doc(jsonBuilder()
                .startObject()
                .field("speed", speed.toString())
                .endObject());
        UpdateResponse updateResponse = client.update(updateRequest).get();
        client.close();
        return updateResponse;
    }

    public IndexResponse createIndexes(String address, int port, String index, String type, String edges, String topic, String streetKey) throws IOException, ExecutionException, InterruptedException {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),
                        9300));
                        IndexRequest indexRequest = new IndexRequest("posts", "doc", "1")
                .source(jsonBuilder().startObject()
                        .field("edges", streetKey + "]")
                        .field("section", topic)
                        .field("speed", "50")
                        .field("keyStreet", streetKey)
                        .endObject());
        IndexResponse indexResponse = client.index(indexRequest).get();
        client.close();
        return indexResponse;
    }

}

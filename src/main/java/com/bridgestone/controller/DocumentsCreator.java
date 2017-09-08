package com.bridgestone.controller;

import com.bridgestone.redis.RedisRepository;
import com.bridgestone.utils.ConfigurationProperties;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by balmung on 08/09/17.
 */
public class DocumentsCreator {

    public void createIndexes(){

        RedisRepository repository = RedisRepository.getInstance();
        repository.connectDB();
        Map<String, String> edges = repository.getAllEdges();
        //creates the list of edges that compose a street in a Json format: NB: it is yet needed the closure ]
        Map<String, String> streets = new HashMap<>();
        for(String streetKey : edges.keySet()){
            if (streets.containsKey(streetKey)){
                //update the street with another edge
                String transientStreet = streets.get(streetKey);
                transientStreet = transientStreet + ",{" + edges.get(streetKey) + "}";
                streets.put(streetKey, transientStreet);
            } else {
                //create the first element of the array and the array itself
                String initialStreet = "edges: [{" + edges.get(streetKey) + "}";
                streets.put(streetKey, initialStreet);
            }
        }

        this.writeIndexes(streets);

        repository.disconnectFromDB();

    }

    private void writeIndexes(Map<String, String> streets){
        System.setProperty("es.set.netty.runtime.available.processors", "false");   //only God knows!!!
        try{
            TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
            for(String streetKey : streets.keySet()){

                IndexResponse response = client.prepareIndex("streets", "street", streetKey)
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("edges", streets.get(streetKey) + "]")
                                .field("section", ConfigurationProperties.TOPIC)
                                .field("speed", "50")
                                .field("keyStreet", streetKey)
                                .endObject()
                        )
                        .get();
                System.err.println("                                            _id = " + response.getId());

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}

package com.bridgestone.utils;

import com.bridgestone.elasticsearch.CloudClient;
import com.bridgestone.redis.RedisRepository;
import org.elasticsearch.action.index.IndexResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by balmung on 08/09/17.
 */
public class DocumentsCreator {

    //search-traffic-analyzer-indexes-faaztbzp3bx3q7hitgjkb44wwi.eu-central-1.es.amazonaws.com
    public static void createIndexes(){

        RedisRepository repository = RedisRepository.getInstance();
        repository.connectDB();
        Map<String, String> edges = repository.getAllEdges();
        //creates the list of edges that compose a street in a Json format: NB: it is yet needed the closure ]
        Map<String, String> streets = new HashMap<>();
        for(String edgeKey : edges.keySet()){
            String streetKey = edges.get(edgeKey);
            if (streets.containsKey(streetKey)){
                //update the street with another edge
                String transientStreet = streets.get(streetKey);
                transientStreet = transientStreet + ",{" + edgeKey + "}";
                streets.put(streetKey, transientStreet);
                System.err.println("ID " + streetKey + " VALORE: " + transientStreet);
            } else {
                //create the first element of the array and the array itself
                String initialStreet = "[{" + edgeKey + "}";
                streets.put(streetKey, initialStreet);
            }
        }

        writeIndexes(streets);

        repository.disconnectFromDB();

    }

    private static void writeIndexes(Map<String, String> streets){
        RedisRepository repository = RedisRepository.getInstance();
        System.setProperty("es.set.netty.runtime.available.processors", "false");   //only God knows!!!
        try{
            //TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
            //.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
            //ElasticClient client = new LocalClient();
            ElasticClient client = new CloudClient();

            for(String streetKey : streets.keySet()){

                /*IndexResponse response = client.prepareIndex("streetindex", "streetinfo", streetKey)
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("edges", streets.get(streetKey)+ "]")
                                .field("section", ConfigurationProperties.TOPIC)
                                .field("speed", "50")
                                .field("keyStreet", streetKey)
                                .endObject()
                        )
                        .get();*/
                //IndexResponse response = client.createIndexes("127.0.0.1", 9300, "streetindex", "streetinfo",streets.get(streetKey), streetKey);
                IndexResponse response = client.createIndexes("localhost", 9300, "streetindex", "streetinfo",
                        streets.get(streetKey), streetKey);
                System.err.println("                                            _id = " + response.getResult() + response.getIndex() + response.getType() + response.getId());
            }
            //client.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }



}

package com.bridgestone.utils;

import com.bridgestone.elasticsearch.CloudClient;
import com.bridgestone.properties.ApplicationProperties;
import com.bridgestone.redis.RedisRepository;
import org.elasticsearch.action.index.IndexResponse;

import java.io.IOException;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by balmung on 08/09/17.
 */
public class DocumentsCreator {

    public static void createIndexes(){

        RedisRepository repository = RedisRepository.getInstance();
        //repository.connectDB();
        Map<String, String> map = new HashMap<>();
        Map<String, String> edges = repository.getAllEdges(map);
        //creates the list of edges that compose a street in a Json format: NB: it is yet needed the closure ]
        Map<String, StreetIndexUtil> streets = new HashMap<>();
        for(String edgeKey : edges.keySet()){
            String streetKey = edges.get(edgeKey);
            if (streets.containsKey(streetKey)){
                //update the street with another edge
                StreetIndexUtil transientStreet = streets.get(streetKey);
                transientStreet.updateEdges(",{" + edgeKey + "}");
                streets.put(streetKey, transientStreet);
                System.err.println("ID " + streetKey + " VALORE: " + transientStreet.getEdges());
            } else {
                //create the first element of the array and the array itself
                StreetIndexUtil initialStreet = new StreetIndexUtil("[{" + edgeKey + "}",
                        getTopicFromEdge(edgeKey));
                streets.put(streetKey, initialStreet);
            }
        }

        writeIndexes(streets);

        repository.disconnectFromDB();

    }

    private static void writeIndexes(Map<String, StreetIndexUtil> streets){
        RedisRepository repository = RedisRepository.getInstance();
        System.setProperty("es.set.netty.runtime.available.processors", "false");   //only God knows!!!
        try{
            //TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
            //.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
            //ElasticClient client = new LocalClient();
            ElasticClient client = new CloudClient();
            String address = ApplicationProperties.getElasticSearchAddress();
            //String address = "search-my-elastic-domain-dioeomsyqpdv2m5yzqghk5wqrq.eu-central-1.es.amazonaws.com";
            for(String streetKey : streets.keySet()) {
                boolean stay = true;
                /* we want to be sure that the indexes are created */
                while (stay) {
                    try {
                        IndexResponse response = client.createIndexes(address, 9300, "streetindex", "streetinfo",
                                streets.get(streetKey).getEdges(), streets.get(streetKey).getTopic(), streetKey);
                        System.err.println("                                            _id = " + response.getResult() + response.getIndex() + response.getType() + response.getId());
                        stay = false;
                    }catch (ConnectException e){
                        e.printStackTrace();
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static String getTopicFromEdge(String edgeKey){
        String[] nodes = edgeKey.split("-");
        String firstNode = nodes[0];
        String[] values = firstNode.split("\\.");
        System.err.println(firstNode);
        String floatingPart1 = values[1];
        System.err.println(floatingPart1);
        floatingPart1 = floatingPart1.substring(0,1);
        String floatingPart2 = values[3];
        floatingPart2 = floatingPart2.substring(0,1);
        return values[0] + "." + floatingPart1 + values[2] + "." + floatingPart2;
    }



}

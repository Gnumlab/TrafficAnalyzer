package com.bridgestone.redis;

import com.bridgestone.entity.Edge;
import com.bridgestone.entity.Node;
import org.redisson.Redisson;
import org.redisson.api.*;

import java.util.*;

/**
 * Created by francesco on 26/07/17.
 */
public class RedisRepository {

    private RedissonClient redissonClient;
    //private RedisConnection<String, String> connection;
    private static RedisRepository instance = new RedisRepository();

    private RedisRepository() {
        this.connectDB();
    }

    public static RedisRepository getInstance() {
        return instance;
    }

    public void connectDB() {
        /*Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://127.0.0.1:6379");*/

        redissonClient = Redisson.create();
    }




    /* Insert a new node into db */
    public void insertNode(Node node){

        //RLock lock = redissonClient.getLock(node.getGraphKey());
        System.err.println("try insert " + node.getGraphKey() + " archi " + node.getNumberOfEdges() );
        Map<String, Node> map = redissonClient.getMap("graphArea");
        map.putIfAbsent(node.getGraphKey(), node);
        //you only want to insert if not present, not updating

        //if (map.putIfAbsent(node.getGraphKey(), node) == null)
            //this.updateNode(node);  /* return null if node is already into db */
    }

    /* replace node into db in order to update the values. This method implies node already exists into db */
    public void updateNode(Node node){
        Map<String, Node> map = redissonClient.getMap("graphArea");
        map.replace(node.getGraphKey(), node);
    }



    /*
    public void insertNode(String nodeKey, Edge newArc){


        Map<String, Edge> map = redissonClient.getMap("graph1");

        map.put(nodeKey, newArc);

        if (newArc != null)
            System.err.println("try insert " + nodeKey + " speed " + newArc.getSpeed() );
        else
            System.err.println("try insert " + nodeKey + " speed " + newArc );


    }
*/

    public void stampa(String key){

        RedissonClient client = Redisson.create();

        System.err.println("staMPAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        Map<String, Node> map = client.getMap("graphArea");
        System.err.println("È " + map.toString());

        Node node = map.get(key);

        //if(arcs != null)


        Map<String, Edge> arcs = node.getEdges();
        Set<String> keys = arcs.keySet();

        for(String chiave : keys){
            System.err.println("    " + arcs.get(chiave).getEndingNode() + arcs.get(chiave).getSpeed());
        }


        /*for(Edge arc : arcs){
            System.err.println(arc.getEndingNode() + "FHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH        RSetMultimap<String, Edge> map = redissonClient.getSetMultimap(\"graphArea\");\n");

        }*/

        client.shutdown();

    }

    public void disconnectFromDB( ) {
        //connection.close();
        redissonClient.shutdown();
    }

    public static void main( String[] args){
        //Redisson.create();
        RedisRepository redisRepository = RedisRepository.getInstance();
        //redisRepository.insertNode(new Node(34.3, 56.8));
    }
}


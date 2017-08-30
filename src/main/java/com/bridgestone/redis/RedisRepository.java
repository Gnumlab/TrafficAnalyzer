package com.bridgestone.redis;

import com.bridgestone.entity.Node;
import com.bridgestone.utils.StreetInfo;
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
    public  void insertNode(Node node){

        RLock lock = redissonClient.getLock(node.getGraphKey());
        lock.lock();
        System.err.println("try insert " + node.getGraphKey() + " archi " + node.getNumberOfEdges() );
        Map<String, Node> map = redissonClient.getMap("graphArea");
        //map.putIfAbsent(node.getGraphKey(), node);
        //you only want to insert if not present, not updating

        map.putIfAbsent(node.getGraphKey(), node);
        lock.unlock();
    }


    /* replace node into db in order to update the values. This method implies node already exists into db */
    public  void updateNode(Node node){
        RLock lock = redissonClient.getLock(node.getGraphKey());
        //while(lock.isLocked()) ;
        lock.lock();
        System.err.println("try update " + node.getGraphKey() + " archi " + node.getNumberOfEdges() );
        Map<String, Node> map = redissonClient.getMap("graphArea");
        map.put(node.getGraphKey(), node);
        lock.unlock();
    }



    public void insertEdge(String edgeKey, String value){
        /**insert an edge into a new "table" of redis. Value will be the key identifiers the street
         * NB: edge key already contains all the information about the edge
         */

        RLock lock = redissonClient.getLock(edgeKey);
        lock.lock();
        Map<String, String> map = redissonClient.getMap("edges");
        //map.putIfAbsent(node.getGraphKey(), node);
        //you only want to insert if not present, not updating

        map.putIfAbsent(edgeKey, value);
        lock.unlock();

    }

    public void updateEdge(String edgeKey, String value){

        RLock lock = redissonClient.getLock(edgeKey);
        lock.lock();
        Map<String, String> map = redissonClient.getMap("edges");
        //map.putIfAbsent(node.getGraphKey(), node);
        //you only want to insert if not present, not updating

        map.put(edgeKey, value);
        lock.unlock();

    }

    public void updateStreetSpeed(String key, StreetInfo value){
        Map<String, StreetInfo> map = redissonClient.getMap("streets");

        System.err.println("                        LUNGHEZZAAAAAAA" + value.getLength());


        map.put(key, value);
    }

    public void insertStreetSpeed(String key, StreetInfo value){
        RLock lock = redissonClient.getLock(key);
        lock.lock();
        Map<String, StreetInfo> map = redissonClient.getMap("streets");
        //map.putIfAbsent(node.getGraphKey(), node);
        //you only want to insert if not present, not updating

        map.putIfAbsent(key, value);
        lock.unlock();
    }

    public String getEdge(String key){
        RLock lock = redissonClient.getLock(key);
        lock.lock();
        Map<String, String> map = redissonClient.getMap("edges");
        lock.unlock();
        return map.get(key);

    }

    public StreetInfo getStreetInfo(String key){
        RLock lock = redissonClient.getLock(key);
        lock.lock();
        Map<String, StreetInfo> map = redissonClient.getMap("streets");
        lock.unlock();
        return map.get(key);


    }



    public RLock getLock (String key){
        return this.redissonClient.getLock(key);
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

    public void stampa(){

        RedissonClient client = Redisson.create();

        System.err.println("staMPAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        Map<String, String> map = client.getMap("streets");
        System.err.println("È " + map.toString());


        //if(arcs != null)


        for(String chiave : map.keySet()){
            System.err.println(" VALORE   " +chiave);
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

    public Node getNode(String nodeKey) {
        RLock lock = redissonClient.getLock(nodeKey);
        lock.lock();

        Map<String, Node> map = redissonClient.getMap("graphArea");

        lock.unlock();
        return map.get(nodeKey);


    }

    public Collection<Node> getAll(){

        Map<String, Node> map = redissonClient.getMap("graphArea");


        return map.values();
    }

    public static void main( String[] args){
        //Redisson.create();
        RedisRepository redisRepository = RedisRepository.getInstance();
        //redisRepository.insertNode(new Node(34.3, 56.8));
    }

    public double getMean(String startKey, String arrivalKey) {

        Map<String, Node> map = redissonClient.getMap("graphArea");

        Node node = map.get(startKey);
        return node.getEdge(arrivalKey). getSpeed();

    }
}


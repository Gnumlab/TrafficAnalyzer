package com.bridgestone.redis;

import com.bridgestone.entity.Node;
import com.bridgestone.properties.ApplicationProperties;
import com.bridgestone.utils.StreetInfo;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.config.Config;

import java.util.*;

/**
 * Created by francesco on 26/07/17.
 */
public class RedisRepository {

    private RedissonClient redissonClient;
    //private RedisConnection<String, String> connection;
    private static RedisRepository instance = new RedisRepository();
    private Config config;

    private RedisRepository() {
        //this.connectDB();
        this.config = new Config();
        /*config.useSingleServer()
                .setAddress("redis://54.93.249.129:6379");*/
        this.config.useClusterServers().setScanInterval(2000).addNodeAddress("redis://52.59.206.94:7000")
                .addNodeAddress("redis://52.59.206.94:7001")
                .addNodeAddress("redis://52.59.206.94:7002");
        /*String address = ApplicationProperties.getRedisAddress();
        this.config.useClusterServers().setScanInterval(2000).addNodeAddress("redis://" + address +
                ":" + ApplicationProperties.getRedisFirstPort())
                .addNodeAddress("redis://" + address + ":" + ApplicationProperties.getRedisSecondPort())
                .addNodeAddress("redis://" + address + ":" + ApplicationProperties.getRedisThirdPort());
        */
    }

    public static RedisRepository getInstance() {
        return instance;
    }

    public void connectDB() {

        redissonClient = Redisson.create(config);
        //redissonClient.getMap("graphArea").delete();
    }




    /* Insert a new node into db */
    public  void insertNode(Node node){
        //this.connectDB();
        RLock lock = redissonClient.getLock(node.getGraphKey());
        lock.lock();
        System.err.println("try insert " + node.getGraphKey() + " archi " + node.getNumberOfEdges() );
        Map<String, Node> map = redissonClient.getMap("graphArea");
        //map.putIfAbsent(node.getGraphKey(), node);
        //you only want to insert if not present, not updating

        map.putIfAbsent(node.getGraphKey(), node);
        lock.unlock();
        //this.disconnectFromDB();
    }


    /* replace node into db in order to update the values. This method implies node already exists into db */
    public  void updateNode(Node node){
        //this.connectDB();
        RLock lock = redissonClient.getLock(node.getGraphKey());
        //while(lock.isLocked()) ;
        lock.lock();
        System.err.println("try update " + node.getGraphKey() + " archi " + node.getNumberOfEdges() );
        Map<String, Node> map = redissonClient.getMap("graphArea");
        map.put(node.getGraphKey(), node);
        lock.unlock();
        //this.disconnectFromDB();
    }



    public void insertEdge(String edgeKey, String value){
        /**insert an edge into a new "table" of redis. Value will be the key identifiers the street
         * NB: edge key already contains all the information about the edge
         */
        //this.connectDB();
        RLock lock = redissonClient.getLock(edgeKey);
        lock.lock();
        Map<String, String> map = redissonClient.getMap("edges");
        //map.putIfAbsent(node.getGraphKey(), node);
        //you only want to insert if not present, not updating

        map.putIfAbsent(edgeKey, value);
        lock.unlock();
        //this.disconnectFromDB();
    }

    public void updateEdge(String edgeKey, String value){
        //this.connectDB();
        RLock lock = redissonClient.getLock(edgeKey);
        lock.lock();
        Map<String, String> map = redissonClient.getMap("edges");
        //map.putIfAbsent(node.getGraphKey(), node);
        //you only want to insert if not present, not updating

        map.put(edgeKey, value);
        lock.unlock();
        //this.disconnectFromDB();
    }

    public void updateStreetSpeed(String key, StreetInfo value){
        //this.connectDB();
        Map<String, StreetInfo> map = redissonClient.getMap("streets");

        System.err.println("                        LUNGHEZZAAAAAAA" + value.getLength());


        map.put(key, value);
        //this.disconnectFromDB();
    }

    public void insertStreetSpeed(String key, StreetInfo value){
        //this.connectDB();
        RLock lock = redissonClient.getLock(key);
        lock.lock();
        Map<String, StreetInfo> map = redissonClient.getMap("streets");
        //map.putIfAbsent(node.getGraphKey(), node);
        //you only want to insert if not present, not updating

        map.putIfAbsent(key, value);
        lock.unlock();
        //this.disconnectFromDB();
    }

    public String getEdge(String key){
        //this.connectDB();
        RLock lock = redissonClient.getLock(key);
        lock.lock();
        Map<String, String> map = redissonClient.getMap("edges");
        String s = map.get(key);
        lock.unlock();
        //this.disconnectFromDB();
        return s;
    }

    public StreetInfo getStreetInfo(String key){
        //this.connectDB();
        RLock lock = redissonClient.getLock(key);
        //lock.lock();
        Map<String, StreetInfo> map = redissonClient.getMap("streets");
        //lock.unlock();
        StreetInfo streetInfo = map.get(key);
        //this.disconnectFromDB();
        return streetInfo;


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
        //this.connectDB();
        RLock lock = redissonClient.getLock(nodeKey);
        lock.lock();

        Map<String, Node> map = redissonClient.getMap("graphArea");

        lock.unlock();
        Node node = map.get(nodeKey);
        //this.disconnectFromDB();
        return node;


    }

    public Collection<Node> getAll(){
        this.connectDB();
        Map<String, Node> map = redissonClient.getMap("graphArea");
        //this.disconnectFromDB();
        return map.values();
    }

    public Map<String, Node> getAllNodes(){
        this.connectDB();
        Map<String, Node> map = redissonClient.getMap("graphArea");
        //this.disconnectFromDB();
        return map;
    }

    public Map<String, String> getAllEdges(Map<String, String> edges){
        this.connectDB();
        Map<String, String> map = redissonClient.getMap("edges");
        for(String key: map.keySet()){
            edges.put(key, map.get(key));
        }
        //this.disconnectFromDB();
        return map;
    }

    public Map<String, StreetInfo> getAllStreets(Map<String, StreetInfo> streets){
        this.connectDB();
        Map<String, StreetInfo> map = redissonClient.getMap("streets");
        for(String key: map.keySet()){
            streets.put(key, map.get(key));
        }
        //this.disconnectFromDB();
        return map;
    }

    public void printEdges(){
        RedisRepository redisRepository = RedisRepository.getInstance();
        Map<String, String> map = new HashMap<>();
        Map<String, String> edges = redisRepository.getAllEdges(map);
        for (String key: edges.keySet()) {
            System.err.print(key + "\t");
            System.err.println(edges.get(key));
        }
    }

    public void printNodes(){
        RedisRepository redisRepository = RedisRepository.getInstance();
        Map<String, Node> nodes = (Map<String, Node>)redisRepository.getAllNodes();
        for (String key: nodes.keySet()) {
            System.err.print(key + "\t");
            System.err.println(nodes.get(key).getNumberOfEdges());
        }
    }

    public void printStreets(){
        RedisRepository redisRepository = RedisRepository.getInstance();
        Map<String, StreetInfo> map = new HashMap<>();
        Map<String, StreetInfo> nodes = (Map<String, StreetInfo>)redisRepository.getAllStreets(map);
        System.err.println(nodes.size());
        for (String key: nodes.keySet()) {
            System.err.print(key + "\t");
            System.err.println(nodes.get(key).getLength());
        }
    }

    public static void main( String[] args){
        //Redisson.create();
        RedisRepository redisRepository = RedisRepository.getInstance();
        /*Node node = new Node(34.3, 56.8);
        redisRepository.insertNode(node);
        System.err.println(redisRepository.getNode(node.getGraphKey()).getGraphKey());*/
        //redisRepository.printEdges();
        redisRepository.printNodes();
        redisRepository.disconnectFromDB();
        System.err.print("sdfsfsfsdfsfsfsdfsfsfsdfsfsfsdfsfsfsdfsfsf");
        redisRepository.printEdges();
        redisRepository.disconnectFromDB();
        System.err.print("sdfsfsfsdfsfsfsdfsfsfsdfsfsfsdfsfsfsdfsfsf");
        redisRepository.printStreets();
        redisRepository.disconnectFromDB();
    }

    public double getMean(String startKey, String arrivalKey) {

        Map<String, Node> map = redissonClient.getMap("graphArea");

        Node node = map.get(startKey);
        return node.getEdge(arrivalKey). getSpeed();

    }
}


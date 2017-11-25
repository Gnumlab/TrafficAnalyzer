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
        this.config = new Config();
        ApplicationProperties.loadProperties();
        String address = ApplicationProperties.getRedisAddress();
        this.config.useClusterServers().setScanInterval(2000).addNodeAddress("redis://" + address +
                ":" + ApplicationProperties.getRedisFirstPort())
                .addNodeAddress("redis://" + address + ":" + ApplicationProperties.getRedisSecondPort())
                .addNodeAddress("redis://" + address + ":" + ApplicationProperties.getRedisThirdPort());
    }

    public static RedisRepository getInstance() {
        return instance;
    }

    public void connectDB() {

        redissonClient = Redisson.create(config);
    }




    /* Insert a new node into db */
    public  void insertNode(Node node){
        RLock lock = redissonClient.getLock(node.getGraphKey());
        lock.lock();
        Map<String, Node> map = redissonClient.getMap("graphArea");
        //you only want to insert if not present, not updating

        map.putIfAbsent(node.getGraphKey(), node);
        lock.unlock();
    }


    /* replace node into db in order to update the values. This method implies node already exists into db */
    public  void updateNode(Node node){
        RLock lock = redissonClient.getLock(node.getGraphKey());
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
        //you only want to insert if not present, not updating

        map.putIfAbsent(edgeKey, value);
        lock.unlock();
    }

    public void updateEdge(String edgeKey, String value){
        RLock lock = redissonClient.getLock(edgeKey);
        lock.lock();
        Map<String, String> map = redissonClient.getMap("edges");
        map.put(edgeKey, value);
        lock.unlock();
    }

    public void updateStreetSpeed(String key, StreetInfo value){
        Map<String, StreetInfo> map = redissonClient.getMap("streets");
        map.put(key, value);
    }

    public void insertStreetSpeed(String key, StreetInfo value){
        RLock lock = redissonClient.getLock(key);
        lock.lock();
        Map<String, StreetInfo> map = redissonClient.getMap("streets");
        map.putIfAbsent(key, value);
        lock.unlock();
    }

    public String getEdge(String key){
        RLock lock = redissonClient.getLock(key);
        lock.lock();
        Map<String, String> map = redissonClient.getMap("edges");
        String s = map.get(key);
        lock.unlock();
        return s;
    }

    public StreetInfo getStreetInfo(String key){
        Map<String, StreetInfo> map = redissonClient.getMap("streets");
        StreetInfo streetInfo = map.get(key);
        return streetInfo;


    }

    public void stampa(){

        RedissonClient client = Redisson.create();

        Map<String, String> map = client.getMap("streets");
        System.err.println("È " + map.toString());

        for(String chiave : map.keySet()){
            System.err.println(" VALORE   " +chiave);
        }

        client.shutdown();

    }

    public void disconnectFromDB( ) {
        redissonClient.shutdown();
    }

    public Node getNode(String nodeKey) {
        RLock lock = redissonClient.getLock(nodeKey);
        lock.lock();

        Map<String, Node> map = redissonClient.getMap("graphArea");

        lock.unlock();
        Node node = map.get(nodeKey);
        return node;


    }

    public Collection<Node> getAll(){
        this.connectDB();
        Map<String, Node> map = redissonClient.getMap("graphArea");
        return map.values();
    }

    public Map<String, Node> getAllNodes(){
        this.connectDB();
        Map<String, Node> map = redissonClient.getMap("graphArea");
        return map;
    }

    public Map<String, String> getAllEdges(Map<String, String> edges){
        this.connectDB();
        Map<String, String> map = redissonClient.getMap("edges");
        for(String key: map.keySet()){
            edges.put(key, map.get(key));
        }
        return map;
    }

    public Map<String, StreetInfo> getAllStreets(Map<String, StreetInfo> streets){
        this.connectDB();
        Map<String, StreetInfo> map = redissonClient.getMap("streets");
        for(String key: map.keySet()){
            streets.put(key, map.get(key));
        }
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

}


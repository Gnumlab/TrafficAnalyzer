package com.bridgestone.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by francesco on 25/07/17.
 */
public class GraphArea { // the entire area managed by a single node
    Map<String, Node> graph;

    public GraphArea() {
        this.graph = new HashMap();
    }

    public Map<String, Node> getGraph() {
        return graph;
    }

    public void setGraph(Map<String, Node> graph) {
        this.graph = graph;
    }

    public void addNode(Node node){
        this.graph.putIfAbsent(node.getGraphKey(),node);
    }

    public void updateNode(Node node){
        this.graph.put(node.getGraphKey(), node);
    }

    public Node getNodeByCoordinates(double x, double y){
        return this.graph.get(Node.makeGraphNodeKey(x,y));
    }

    public Node getNodeByKey(String key) {
        return this.graph.get(key);
    }

    public boolean contains(Node node){
        return this.graph.containsKey(Node.makeGraphNodeKey(node.getX(), node.getY()));
    }



}

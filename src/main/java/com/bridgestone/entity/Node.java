package com.bridgestone.entity;

import java.util.HashMap;
import java.util.Set;

/**
 * Created by francesco on 25/07/17.
 */
public class Node {

    /** x and y are expected to be the starting coordinates of the section in exam:
     * they will be the coordinates approximated to a certain decimal precision
      */
    private double x;
    private double y;
    private HashMap<String, Edge> edges; // vectors which starts with the graph section in question
    /* how to know if there is a split way?: if i have a bidirectional
    if I have bidirectional ore one way street then they will have in their list of vectors at most two
    vectors -> simple control: if vectors.len >= 3 then there is a split.
    N.B: in vectors there should be only the kind of vectors possessing the 'startSection' the same as current
    graph section */
    private String graphKey; // it will be useful for graph purposes

    public Node(){
        this.edges = new HashMap<>();
    }
    public Node(double x, double y) {
        this.x = x;
        this.y = y;
        this.graphKey = Node.makeGraphNodeKey(x, y);
        this.edges = new HashMap<>();
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public String getGraphKey() {
        return graphKey;
    }

    public void setGraphKey(String graphKey) {
        this.graphKey = graphKey;

    }

    public static String makeGraphNodeKey(double x, double y){
        return "(" + Double.toString(x) + "," + Double.toString(y) + ")";
    }

    public HashMap<String, Edge> getEdges() {
        return edges;
    }

    public void addEdge(Edge edge){
        this.edges.putIfAbsent(Edge.makeGraphEdgeKey(edge), edge);
    }

    public void updateEdge(Edge edge){
        this.edges.put(Edge.makeGraphEdgeKey(edge), edge);
    }

    public Edge getEdge(String key){
        return this.edges.get(key);
    }

    public int getNumberOfEdges(){
        return this.edges.size();
    }




    public void printEdge(){
        System.err.println("                STAMPA DEGLI ARCHI MIEI SIGNORI!");

        Set<String> keys = edges.keySet();
        if (keys == null) {
            System.err.println("                ENULL TEST'IMMERD MIEI SIGNORI!");
            return;
        }

        for(String chiave : keys){
            System.err.println(edges.get(chiave).getEndingNode() + edges.get(chiave).getSpeed());
        }
    }

}

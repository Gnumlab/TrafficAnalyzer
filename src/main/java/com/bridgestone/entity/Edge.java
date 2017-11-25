package com.bridgestone.entity;

/**
 * Created by francesco on 25/07/17.
 */
public class Edge {

    //private Node startNode; // don't know if necessary: probably not;
    // but it's stylish to have simmetry
    private String startNode;
    private String endingNode; // the section pointed at the arrival of data

    private double speed; // probably a mean speed while receiving data?


    public Edge(){}

    public Edge(String startNode, String endingNode, int speed) {
        this.startNode = startNode;
        this.endingNode = endingNode;
        this.speed = speed;
    }

    public String getStartNode() {
        return startNode;
    }

    public String getEndingNode() {
        return endingNode;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public static String makeGraphEdgeKey(Edge edge){
        return edge.getStartNode() + "-" + edge.getEndingNode();
    }

}

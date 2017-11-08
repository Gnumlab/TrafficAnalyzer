package com.bridgestone.utils;

/**
 * Created by francesco on 26/09/17.
 */
public class StreetIndexUtil {
    private String edges; //jason-array of the list of edges
    private String topic; // section to witch the list belong

    public StreetIndexUtil(String edges, String topic) {
        this.edges = edges;
        this.topic = topic;
    }

    public String getEdges() {
        return edges;
    }

    public void setEdges(String edges) {
        this.edges = edges;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void updateEdges(String updatingString){
        this.edges = this.edges + updatingString;
    }
}

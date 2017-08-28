package com.bridgestone.utils;

import com.bridgestone.entity.Edge;
import com.bridgestone.entity.Node;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Created by balmung on 28/08/17.
 */
public class JSonParser {


    public static Node makeNode(JsonNode jsonData, int nodeIndex){
        String index = Integer.toString(nodeIndex);
        double x = jsonData.get("x" + index).asDouble();
        double y = jsonData.get("y" + index).asDouble();
        return new Node(x, y);
    }

    public static Edge makeEdge(JsonNode jsonData, Node startingNode, Node arrivalNode){

        int speed = jsonData.get("speed").asInt();
        return new Edge(startingNode.getGraphKey(), arrivalNode.getGraphKey(), speed);
    }
}

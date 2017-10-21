package com.bridgestone.controller;

import com.bridgestone.entity.Edge;
import com.bridgestone.entity.GraphArea;
import com.bridgestone.entity.Node;
import com.bridgestone.redis.RedisRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Collection;

/**
 * Created by francesco on 25/07/17.
 */
public class TopologyGraphController {

    private GraphArea topology;
    private ObjectMapper mapper;
    private RedisRepository repository;


    private static final TopologyGraphController instance = new TopologyGraphController();

    private TopologyGraphController(){
        this.topology = new GraphArea();
        this.mapper = new ObjectMapper();
        this.repository = RedisRepository.getInstance();
        this.repository.connectDB();
    }

    public static TopologyGraphController getInstance(){
        return instance;
    }


    public void updateGraphTopology(String jsonData) throws IOException {


        //this.repository.connectDB();

        JsonNode msg = mapper.readTree(jsonData);
        Node startingNode = this.makeNode(msg, 1);
        Node arrivalNode = this.makeNode(msg, 2);

        Edge edge = this.makeEdge(msg, startingNode, arrivalNode);
        System.out.println("UPDATE GRAPH " + edge.getStartNode() +"\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");

        /**IMPORTANT:
         * the GraphArea method 'addSection()' uses the 'putIfAbsent()' operation of the HashMap class:
         * even if a section is already in the graph, the addSection method poses no problem
         * since it won't insert the section a second time
         */


        synchronized (this) {
            //TopologyGtaphController is a singleton then this is unique
            if (!this.topology.contains(startingNode)) {
                System.err.println("Non c0è nulla");
                // new section and edge to insert
                //remember vectors are added to the starting size
                startingNode.addEdge(edge);
                this.topology.addNode(startingNode);
                this.repository.insertNode(startingNode);
                this.topology.addNode(arrivalNode); // even the arrival section might be new !!!
                this.repository.insertNode(arrivalNode);


            } else if (!this.topology.contains(arrivalNode)) {
                System.err.println("Non solo uno nulla");
                //the starting section exists but not the arrival one: there is always a new edge to insert

                Node updatingNode = this.getNodeByCoordinates(startingNode.getX(), startingNode.getY()); //taking the section to add the edge
                updatingNode.addEdge(edge);

                this.topology.updateNode(updatingNode);
                this.repository.updateNode(updatingNode);

                this.topology.addNode(arrivalNode);
                this.repository.insertNode(arrivalNode);


            } else {//both the sections exists
                System.err.println("ALLL INNNNNNNNNNNN");
                //the edge may not exists: create or updating the mean speed of the already existing edge
                Node updatingNode = this.getNodeByCoordinates(startingNode.getX(), startingNode.getY()); //taking the section containing the edge

                // taking and updating the edge
                Edge updatingEdge = updatingNode.getEdge(Edge.makeGraphEdgeKey(edge));

                if (updatingEdge == null) {
                    //case: the edge doesn't exist

                    /** edges always inserted in both sides: if the first doesn't exist so it won't the other one
                     *  there is need to update both nodes
                     */
                    arrivalNode = this.topology.getNodeByKey(arrivalNode.getGraphKey());

                    updatingNode.addEdge(edge);

                    /*updatingEdge.setSpeed(updatingEdge.getSpeed() * 0.4 + edge.getSpeed() * 0.6);
                    updatingNode.updateEdge(updatingEdge);*/
                    this.topology.updateNode(updatingNode);
                    this.repository.updateNode(updatingNode);

                }

            }
        }

        //this.repository.stampa();

        //this.repository.disconnectFromDB();


    }

    public Node getNodeByCoordinates(double x, double y){
        return this.topology.getNodeByCoordinates(x, y);
    }

    private Node makeNode(JsonNode jsonData, int nodeIndex){
        String index = Integer.toString(nodeIndex);
        double x = jsonData.get("x" + index).asDouble();
        double y = jsonData.get("y" + index).asDouble();
        return new Node(x, y);
    }

    private Edge makeEdge(JsonNode jsonData, Node startingNode, Node arrivalNode){

        int speed = jsonData.get("speed").asInt();
        return new Edge(startingNode.getGraphKey(), arrivalNode.getGraphKey(), speed);
    }


    //if a node has more than 2 starting vectors it is a crossroad
    public boolean checkIfCrossroad(Node node){
        if(node.getNumberOfEdges() > 2)
            return true;
        return false;
    }

    private Edge getForwardEdge(Node start, Node arrival){
        // taking all the vectors starting from the arrival section
        Collection<Edge> edges = arrival.getEdges().values();
        Edge nextEdge = null;
        for ( Edge edge : edges) {
            if ( !edge.getEndingNode().equals(start.getGraphKey())) {//NB: ending node is now the key of the Node
            /*if the ending side of the vector is the one where I start than it is not the next vector
              but a back-propaagtion vector
             */
                nextEdge = edge;
                break;
            }
        }
        return nextEdge;
    }

    /** N.B: the results will be updated into the instance result of CalculateMeanResult */
    private void recursiveMeanCalculation(CalculateMeanResult result,
                                                         Edge startingEdge, Node startingNode){

        // updating the values of result for the current vector analyzed
        result.sum = startingEdge.getSpeed();
        result.numElem ++;
        Node arrivalNode = this.topology.getNodeByKey(startingEdge.getEndingNode());
        if(checkIfCrossroad(arrivalNode)){
            /*if next section is a crossroad this is the last value to sum:
            exit condition for the recursion
             */
            return;
        }
        //propagation of the operation until a crossroad
        //updating starting vector to the new vector to process
        startingEdge = this.getForwardEdge(startingNode, arrivalNode);
        this.recursiveMeanCalculation(result, startingEdge, arrivalNode);
    }


    private double calculateSpeedMean(Node node){
        CalculateMeanResult result = new CalculateMeanResult(0, 0);
        Collection<Edge> edges = node.getEdges().values();
        /*taking all the starting vectors: at most two if bidirectional sense of traffic:
          already excluded the case > 2
         */
        for ( Edge edge : edges) {
            //recursiveMeanCalculation() will make the sum of all the speed values and number of values added
            this.recursiveMeanCalculation(result, edge, node);
        }
        return result.sum/result.numElem; //return the total mean
    }

    //returns the mean speed value of the road selected
    public double calculateMeanOfRoad(double x, double y){
        //x and y: coordinates from which starting to calculate the mean
        Node node = this.getNodeByCoordinates(x, y);
        if(checkIfCrossroad(node)){
            // handle the specific case in which the starting node is a crossroad
            //yet to implement
        }
        return calculateSpeedMean(node);
    }

    private class CalculateMeanResult{
        private double sum; // sum of the vector values of speed
        private int numElem; // number of values added

        private CalculateMeanResult(double sum, int numElem){
            this.sum = sum;
            this.numElem = numElem;
        }
    }

}

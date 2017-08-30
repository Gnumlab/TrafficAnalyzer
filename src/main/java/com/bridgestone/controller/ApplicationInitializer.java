package com.bridgestone.controller;

import com.bridgestone.entity.Edge;
import com.bridgestone.entity.GraphArea;
import com.bridgestone.entity.Node;
import com.bridgestone.redis.RedisRepository;
import com.bridgestone.utils.StreetInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;

import java.util.*;

/**
 * Created by balmung on 28/08/17.
 */
public class ApplicationInitializer {

    private GraphArea graph;
    private HashMap<String, Edge> standByEdges;     //edges that are met but are not inserted into a street
    private HashMap<String, Edge> processed;

    private RedisRepository repository;
    private ApplicationInitializer() {
        this.repository = RedisRepository.getInstance();
        this.graph = new GraphArea();
        this.standByEdges = new HashMap<>();
        this.processed = new HashMap<>();
    }



    public void createGraph(){

        Collection<Node> nodes =  repository.getAll();
        for(Node node: nodes){
            System.err.println("nodo: " + node.getGraphKey() + "archi: " + node.getNumberOfEdges());

            this.graph.addNode(node);
        }

    }

    public void createStreets(){

        TopologyGraphController topologyGraphController = TopologyGraphController.getInstance();

        Map<String, Node> nodesMap = graph.getGraph();
        Collection<Node> nodes = nodesMap.values();
        Integer key = new Integer("0");
        for(Node node: nodes){

            Collection<Edge> edges = node.getEdges().values();
            if (topologyGraphController.checkIfCrossroad(node)){
                for(Edge edge: edges){
                    streetCalc(edge, node, key, 1);
                    key++;
                }

            }
            else{

                for(Edge edge: edges){
                    this.standByEdges.put(Edge.makeGraphEdgeKey(edge), edge);
                }
            }
        }

        Collection<Edge> edges = this.standByEdges.values();

        for(Edge edge : edges){
            if(!this.processed.containsKey(Edge.makeGraphEdgeKey(edge))) {
                Node node = this.graph.getNodeByKey(edge.getStartNode());
                if (node.getNumberOfEdges() < 2) {
                    //node has only one outcoming edge so it could be a start of a street
                    streetCalc(edge, node, key, 1);
                    key++;
                }
            }
        }

        this.repository.stampa();
        this.repository.disconnectFromDB();



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
    private void streetCalc(Edge startingEdge, Node startingNode, Integer key, int length){
        // updating the values of result for the current vector analyzed

        System.err.print("STREET CALC " +startingEdge.getEndingNode());


        Node arrivalNode = this.graph.getNodeByKey(startingEdge.getEndingNode());

        /*write arrivalNode into redis with key*/
        this.repository.updateEdge(Edge.makeGraphEdgeKey(startingEdge), key.toString());
        //this.standByEdges.remove(Edge.makeGraphEdgeKey(startingEdge));
        this.processed.putIfAbsent(Edge.makeGraphEdgeKey(startingEdge), startingEdge);

        System.err.println("FINEEE                   fineeeeeeeeeeeee " + key + " " + startingNode.getGraphKey());


        if(checkIfCrossroad(arrivalNode)){
            /*if next section is a crossroad this is the last value to sum:
            exit condition for the recursion
             */

            System.err.println("FINEEE                   fineeeeeeeeeeeee " + key + " " + startingNode.getGraphKey());
            this.repository.insertStreetSpeed(key.toString(), new StreetInfo(0, length));

            return;
        }


        //propagation of the operation until a crossroad
        //updating starting vector to the new vector to process
        startingEdge = this.getForwardEdge(startingNode, arrivalNode);
        if( startingEdge == null) {

            System.err.println("FINEEE                   fineeeeeeeeeeeee " + key + " " + startingNode.getGraphKey());


            this.repository.insertStreetSpeed(key.toString(), new StreetInfo(0, length));

            return;
        }

        this.streetCalc(startingEdge, arrivalNode, key, ++length);
    }



    public static void main(String args[]){
        ApplicationInitializer app = new ApplicationInitializer();
        app.createGraph();
        app.createStreets();
        System.err.println("FINE");
    }

}

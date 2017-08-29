package com.bridgestone.controller;

import com.bridgestone.entity.Edge;
import com.bridgestone.entity.GraphArea;
import com.bridgestone.entity.Node;
import com.bridgestone.redis.RedisRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;

import java.util.*;

/**
 * Created by balmung on 28/08/17.
 */
public class ApplicationInitializer {

    private GraphArea graph;

    private RedisRepository repository;
    private ApplicationInitializer() {
        this.repository = RedisRepository.getInstance();
        this.graph = new GraphArea();
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

        int cross = 0, nocross = 0, total = 0;
        for(Node node: nodes){

            total++;
            System.err.print("È incrocio? ");
            Collection<Edge> edges = node.getEdges().values();
            if (topologyGraphController.checkIfCrossroad(node)){
                System.err.println("Incrocio!" + node.getGraphKey());
                cross++;
                for(Edge edge: edges){
                    streetCalc(edge, node, key);
                    key++;
                }

            }
            else{
                System.err.println("No incrocio!" + node.getNumberOfEdges());
                nocross++;

                for(Edge edge: edges){

                    this.repository.insertEdge(Edge.makeGraphEdgeKey(edge), key.toString());
                    this.repository.insertStreetSpeed(key.toString(), new Double("0"));
                    key++;
                }

            }

        }

        System.out.println(total +" "+ nocross +" " + cross);
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
    private void streetCalc(Edge startingEdge, Node startingNode, Integer key){
        // updating the values of result for the current vector analyzed

        System.err.print("STREET CALC " +startingEdge.getEndingNode());


        Node arrivalNode = this.graph.getNodeByKey(startingEdge.getEndingNode());




        if(checkIfCrossroad(arrivalNode)){
            /*if next section is a crossroad this is the last value to sum:
            exit condition for the recursion
             */
            return;
        }

        /*write arrivalNode into redis with key*/
        this.repository.updateEdge(Edge.makeGraphEdgeKey(startingEdge), key.toString());
        this.repository.updateStreetSpeed(key.toString(), new Double("0"));
        //propagation of the operation until a crossroad
        //updating starting vector to the new vector to process
        startingEdge = this.getForwardEdge(startingNode, arrivalNode);
        if( startingEdge == null)
            return;
        this.streetCalc(startingEdge, arrivalNode, key);
    }



    public static void main(String args[]){
        ApplicationInitializer app = new ApplicationInitializer();
        app.createGraph();
        app.createStreets();
        System.err.println("FINE");
    }

}

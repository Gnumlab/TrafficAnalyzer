package com.bridgestone.bolt;

import com.bridgestone.entity.Edge;
import com.bridgestone.entity.Node;
import com.bridgestone.redis.RedisRepository;
import com.bridgestone.utils.JSonParser;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.Map;

/**
 * Created by francesco on 24/07/17.
 */
public class MeanCalculatorBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private ObjectMapper mapper;

    private RedisRepository repository;


    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        mapper = new ObjectMapper();
        _collector = collector;
        this.repository = RedisRepository.getInstance();
    }

    @Override
    public void execute(Tuple tuple) {
        /*System.err.println("received" + tuple.getString(0) + "!!!" + "\n\n\n\n\n\n\n\n\n\n");
        _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
        _collector.ack(tuple);*/
        String jsonData = tuple.getString(0);
        try {
            JsonNode msg = mapper.readTree(jsonData);

            System.err.print(" RICEVUTO"+ msg);


            Node startingNode = JSonParser.makeNode(msg, 1);
            Node arrivalNode = JSonParser.makeNode(msg, 2);

            startingNode = this.repository.getNode(startingNode.getGraphKey());
            Edge edge = JSonParser.makeEdge(msg, startingNode, arrivalNode);
            edge = startingNode.getEdge(Edge.makeGraphEdgeKey(edge));

            if(edge == null){// a new edge
                System.err.println(" LUI NON HA EDGE"+ msg +"\n\n\n\n\n\n\n\n\n\n");
                //startingNode.addEdge(edge); //insertig new node
            } else {

                edge.updateSpeed(msg.get("speed").asDouble());
            }

            this.repository.updateNode(startingNode); // saving the updated node in repository
            _collector.emit(new Values(Edge.makeGraphEdgeKey(edge), edge.getSpeed()));

            /*Edge edge = JSonParser.makeEdge(msg, startingNode, arrivalNode);

            String streetKey = this.repository.getEdge(Edge.makeGraphEdgeKey(edge));
            Double speed = this.repository.getStreetSpeed(streetKey);

            this.repository.updateStreetSpeed(streetKey, speed*0.4 + edge.getSpeed()*0.6);*/

        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            _collector.ack(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("edge", "speed"));
    }


}

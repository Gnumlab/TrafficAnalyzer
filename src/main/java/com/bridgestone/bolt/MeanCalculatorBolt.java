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
    /**despite the name, the class i ss responsible for taking the information from the json tuple*/
    private OutputCollector _collector;
    private ObjectMapper mapper;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        mapper = new ObjectMapper();
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String jsonData = tuple.getString(0);
        try {
            JsonNode msg = mapper.readTree(jsonData);

            Node startingNode = JSonParser.makeNode(msg, 1);
            Node arrivalNode = JSonParser.makeNode(msg, 2);
            Edge edge = JSonParser.makeEdge(msg, startingNode, arrivalNode);
            _collector.emit(new Values(Edge.makeGraphEdgeKey(edge), msg.get("speed").asDouble()));

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

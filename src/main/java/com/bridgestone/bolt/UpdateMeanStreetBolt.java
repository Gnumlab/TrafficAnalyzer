package com.bridgestone.bolt;

import com.bridgestone.entity.Edge;
import com.bridgestone.entity.Node;
import com.bridgestone.redis.RedisRepository;
import com.bridgestone.utils.JSonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Created by balmung on 28/08/17.
 */
public class UpdateMeanStreetBolt extends BaseRichBolt {
    OutputCollector _collector;
    ObjectMapper mapper;

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
        System.err.println("Update STreet BOLT" + tuple.getString(0) + "!!!" + "\n\n\n\n\n\n\n\n\n\n");

        String streetKey = this.repository.getEdge(tuple.getString(0));
        Double speed = this.repository.getStreetSpeed(streetKey);

        this.repository.updateStreetSpeed(streetKey, speed*0.4 + tuple.getDouble(1)*0.6);

        //_collector.emit(new Values(Edge.makeGraphEdgeKey(edge), edge.getSpeed()));


        _collector.ack(tuple);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("edge", "speed"));
    }

}

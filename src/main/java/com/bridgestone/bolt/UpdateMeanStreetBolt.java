package com.bridgestone.bolt;

import com.bridgestone.redis.RedisRepository;
import com.bridgestone.utils.StreetInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by balmung on 28/08/17.
 */
public class UpdateMeanStreetBolt extends BaseRichBolt {
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
        System.err.println("Update STreet BOLT" + tuple.getString(0) + "!!!" + "\n\n\n\n\n\n\n\n\n\n");

        String edge = tuple.getString(0);

        String streetKey = this.repository.getEdge(edge);
        StreetInfo streetInfo = this.repository.getStreetInfo(streetKey);
        streetInfo.updateSpeed(tuple.getDouble(1));
        this.repository.updateStreetSpeed(streetKey, streetInfo);

        _collector.emit(new Values(streetKey, streetInfo.getSpeed()));


        _collector.ack(tuple);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("street", "speed"));
    }

}

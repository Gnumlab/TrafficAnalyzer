package com.bridgestone.bolt;

import com.bridgestone.kafka.StreetProducer;
import com.bridgestone.redis.RedisRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by balmung on 30/08/17.
 */
public class ProducerBolt extends BaseRichBolt{
    OutputCollector _collector;
    ObjectMapper mapper;

    StreetProducer streetProducer;




    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        mapper = new ObjectMapper();
        _collector = collector;
        this.streetProducer = new StreetProducer();
    }

    @Override
    public void execute(Tuple tuple) {
        String edge = tuple.getStringByField("edge");
        Double speed = tuple.getDoubleByField("speed");

        streetProducer.send(edge, speed);

        _collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

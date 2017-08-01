package com.bridgestone.storm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

/**
 * Created by francesco on 24/07/17.
 */
public class MeanCalculatorBolt extends BaseRichBolt {
    OutputCollector _collector;
    ObjectMapper mapper;

    static double speedMean;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        mapper = new ObjectMapper();
        _collector = collector;
        speedMean = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        /*System.err.println("received" + tuple.getString(0) + "!!!" + "\n\n\n\n\n\n\n\n\n\n");
        _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
        _collector.ack(tuple);*/
        System.err.println("received" + tuple.getString(0) + "!!!" + "\n\n\n\n\n\n\n\n\n\n");
        String jsonData = tuple.getString(0);
        try {
            JsonNode msg = mapper.readTree(jsonData);
            int speed = msg.get("speed").asInt();
            synchronized (MeanCalculatorBolt.class) {
                speedMean = speedMean * 1 + speed * 1;
                System.err.print("                              " + speedMean + "\n\n\n\n\n\n\n\n\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            _collector.ack(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}

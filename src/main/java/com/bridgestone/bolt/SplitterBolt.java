package com.bridgestone.bolt;


import com.bridgestone.entity.Edge;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;

/**
 * Created by balmung on 29/08/17.
 */
public class SplitterBolt extends BaseRichBolt {

    private OutputCollector _collector;


    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }


    @Override
    public void execute(Tuple tuple) {

        String jsonData = tuple.getString(0);

        JSONParser parser = new JSONParser();
        try {
            Object object = parser.parse(jsonData);

            JSONArray data = (JSONArray) object;

            for(int i = 0; i < data.size(); i++){

                _collector.emit(new Values(data.get(i).toString()));

            }

        } catch (ParseException e) {
            e.printStackTrace();
        } finally {
            _collector.ack(tuple);
        }

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("edge"));
    }
}

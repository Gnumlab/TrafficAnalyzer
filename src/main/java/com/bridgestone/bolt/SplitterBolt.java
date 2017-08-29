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

import javax.json.*;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

/**
 * Created by balmung on 29/08/17.
 */
public class SplitterBolt extends BaseRichBolt {
    OutputCollector _collector;
    ObjectMapper mapper;


    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        mapper = new ObjectMapper();
        _collector = collector;
    }


    @Override
    public void execute(Tuple tuple) {
        System.err.println("received" + tuple.getString(0) + "!!!" + "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
        String jsonData = tuple.getString(0);

        /*JsonNode msg = mapper.readTree(jsonData);
        JsonNode data = msg.get("data");
        System.err.println("WHAT I RECEIVED " + data.textValue());*/

        JSONParser parser = new JSONParser();
        try {
            Object object = parser.parse(jsonData);

            JSONArray data = (JSONArray) object;

            for(int i = 0; i < data.size(); i++){


                _collector.emit(new Values(data.get(i).toString()));

                System.err.println("WHAT I RECEIVED " + data.get(i).toString());
            }

        } catch (ParseException e) {
            e.printStackTrace();
        } finally {
            _collector.ack(tuple);
        }


        /*JsonReader reader = Json.createReader(new StringReader(jsonData));
        JsonObject data = reader.readObject();
        reader.close();

        System.err.println("WHAT I RECEIVED " + data.toString());
        JsonValue record = data.get("data");

        for(int i  = 0; i < record.size(); i++) {

            System.err.println("WHAT I   " + record.toString());
        }*/

        /*for(int i = 0; i < data.size(); i++){
            JsonNode record = data.get(i);
            System.err.println("WHAT I RECEIVED " + record.textValue());
        }*/
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("edge"));
    }
}

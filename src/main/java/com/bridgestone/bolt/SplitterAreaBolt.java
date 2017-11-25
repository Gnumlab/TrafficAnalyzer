
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

import java.io.IOException;
import java.util.Map;

/**
 * Created by balmung on 29/08/17.
 */
public class SplitterAreaBolt extends BaseRichBolt {
    /**it receives the array of arcs and splits them, sending single arcs to the next bolt */

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

        JSONParser parser = new JSONParser();
        try {
            Object object = parser.parse(jsonData);

            JSONArray data = (JSONArray) object;

            for(int i = 0; i < data.size(); i++){
                String jsonObject = data.get(i).toString();
                JsonNode msg = mapper.readTree(jsonData);
                String x1 = Double.toString(msg.get("x1").asDouble());
                String y1 = Double.toString(msg.get("y1").asDouble());
                _collector.emit(new Values(jsonObject, x1 + y1));
            }

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        } finally {
            _collector.ack(tuple);
        }

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("edge", "fieldgroupkey"));
    }
}

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

import java.util.HashMap;
import java.util.Map;

/**
 * Created by balmung on 28/08/17.
 */
public class UpdateMeanStreetBolt extends BaseRichBolt {
    /**Bolt responsible for the updating of the speed value of an arc */

    private OutputCollector _collector;
    private ObjectMapper mapper;
    private Map<String, String> edgesToStreet = new HashMap<>();
    private Map<String, StreetInfo> streetSpeed = new HashMap<>();
    private RedisRepository repository;


    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        mapper = new ObjectMapper();
        _collector = collector;
        this.repository = RedisRepository.getInstance();
        synchronized (this.repository) {
            boolean prepared = false;
            while(!prepared) {
                try {
                    /* getting all the needed data from the beginning */
                    this.repository.getAllEdges(this.edgesToStreet);
                    this.repository.getAllStreets(this.streetSpeed);
                    this.repository.disconnectFromDB();
                    prepared = true;
                    /* exiting only if all the data are retrieved, else it will retry until
                       no exception are met */
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @Override
    public void execute(Tuple tuple) {

        String edge = tuple.getString(0);

        try {
            String streetKey = this.edgesToStreet.get(edge);
            StreetInfo streetInfo = this.streetSpeed.get(streetKey);
            streetInfo.updateSpeed(tuple.getDouble(1));
            this.streetSpeed.put(streetKey, streetInfo);

            _collector.emit(new Values(streetKey, streetInfo.getSpeed()));

        }catch(Exception e){
            e.printStackTrace();
        }finally {
            _collector.ack(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("street", "speed"));
    }

}

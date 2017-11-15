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
            /** done only once for all the threads of the same worker */
            boolean prepared = false;
            while(!prepared) {
                try {
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
        //boolean stay = true;
        /*while(stay) {
            try {
                this.repository.connectDB();
                stay = false;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }*/
    }

    @Override
    public void execute(Tuple tuple) {
        System.err.println("Update STreet BOLT" + tuple.getString(0) + "!!!" + "\n\n\n\n\n\n\n\n\n\n");

        String edge = tuple.getString(0);

        //this.repository.connectDB();
        try {
            String streetKey = this.edgesToStreet.get(edge);
            StreetInfo streetInfo = this.streetSpeed.get(streetKey);
            streetInfo.updateSpeed(tuple.getDouble(1));
            this.streetSpeed.put(streetKey, streetInfo);
            //this.repository.updateStreetSpeed(streetKey, streetInfo);

            //this.repository.disconnectFromDB();

            _collector.emit(new Values(streetKey, streetInfo.getSpeed()));

        }catch(Exception e){
            e.printStackTrace();
            System.err.print("\n\n\n\n\n\n\n\n\n\n");
        }finally {
            _collector.ack(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("street", "speed"));
    }

}

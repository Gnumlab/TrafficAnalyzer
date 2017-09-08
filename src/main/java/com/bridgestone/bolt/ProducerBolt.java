package com.bridgestone.bolt;

import com.bridgestone.kafka.StreetProducer;
import com.bridgestone.redis.RedisRepository;
import com.bridgestone.utils.ConfigurationProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by balmung on 30/08/17.
 */
public class ProducerBolt extends BaseRichBolt{
    OutputCollector _collector;
    ObjectMapper mapper;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        mapper = new ObjectMapper();
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String streetKey = tuple.getStringByField("streetKey");
        Double speed = tuple.getDoubleByField("speed");
        try{
            TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index("streetIndex");
            updateRequest.type("streetInfo");
            updateRequest.id(streetKey);
            updateRequest.doc(jsonBuilder()
                    .startObject()
                    .field("speed", speed.toString())
                    .endObject());
            client.update(updateRequest).get();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            _collector.ack(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

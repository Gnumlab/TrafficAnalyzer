package com.bridgestone.bolt;

import com.bridgestone.elasticsearch.CloudClient;
import com.bridgestone.elasticsearch.LocalClient;
import com.bridgestone.utils.ElasticClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by balmung on 30/08/17.
 */
public class ProducerBolt extends BaseRichBolt{
    /** the class ProducerBolt is responsible for the updating of the data indexed in Elasticsearch*/
    private OutputCollector _collector;
    private ObjectMapper mapper;
    private ReleasableLock releasableLock;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        mapper = new ObjectMapper();
        _collector = collector;
        ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
        releasableLock = new ReleasableLock(rwl.writeLock());
    }

    @Override
    public void execute(Tuple tuple) {
        System.setProperty("es.set.netty.runtime.available.processors", "false");   //only God knows!!!

        String streetKey = tuple.getStringByField("street");
        Double speed = tuple.getDoubleByField("speed");
        try{

            releasableLock.acquire();

            //ElasticClient client = new LocalClient();
            ElasticClient client = new CloudClient();
            client.updateSpeedStreet("traffic-analyzer-indexes.eu-central-1.es.amazonaws.com", 9300, "streetindex", "streetinfo",
                    streetKey, speed);
           /* UpdateResponse updateResponse = client.prepareUpdate("streetindex", "streetinfo", streetKey)
                    .setScript(new Script("ctx._source.speed=\"" + speed + "\""))
                    .execute()
                    .actionGet();*/
            //System.err.println("hhhhhhhhhhhhhhgghghghghhhhhhhhh            " + updateResponse.getGetResult().field("speed").getValue()+ "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
            releasableLock.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            _collector.ack(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

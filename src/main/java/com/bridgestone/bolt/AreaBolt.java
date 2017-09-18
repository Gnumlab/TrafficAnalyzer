package com.bridgestone.bolt;

import com.bridgestone.controller.TopologyGraphController;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Created by francesco on 26/07/17.
 */
public class AreaBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private ObjectMapper mapper;
    private TopologyGraphController controller;

    static double count = 0;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        mapper = new ObjectMapper();
        _collector = collector;
        controller = TopologyGraphController.getInstance();
    }

    @Override
    public void execute(Tuple tuple) {
        System.err.println("received" + tuple.getString(0) + "!!!" + "\n\n\n\n\n\n\n\n\n\n");
        String jsonData = tuple.getString(0);
        try {
            this.controller.updateGraphTopology(jsonData);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            this.add();
            _collector.ack(tuple);
        }
    }

    public synchronized void add(){
        count++;
        System.err.println("COUNTERLEREIRAORSAOFR           " + count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }


    public static void  runTopologyGraph(){
        TopologyBuilder builder = new TopologyBuilder();
        String zkConnString = "54.93.96.33:2181";
        double x, y;
        x = 52.12;
        y = 41.34;
        String topic = Double.toString(x) + Double.toString(y);
        BrokerHosts hosts = new ZkHosts(zkConnString);

        //new StreetProducer().createTopic(topic);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,
                UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());


        builder.setSpout("StreetInfo", new KafkaSpout(kafkaSpoutConfig),10);
        //parallelism hint: number of thread for node
        //builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("StreetInfo");
        //builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");
        builder.setBolt("split", new SplitterBolt(),10).shuffleGrouping("StreetInfo");
        builder.setBolt("mean", new AreaBolt(),10).shuffleGrouping("split");


        Config conf = new Config();
        conf.setDebug(false);




        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        System.err.print("Submitted topology " + topic + "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
        Utils.sleep(1000000);
        cluster.killTopology("test");
        cluster.shutdown();
    }



    public static void main(String[] args) throws Exception {

        /*TopologyBuilder builder = new TopologyBuilder();
        String zkConnString = "54.93.96.33:2181";
        double x, y;
        x = 52.12;
        y = 41.34;
        String topic = Double.toString(x) + Double.toString(y);
        BrokerHosts hosts = new ZkHosts(zkConnString);

        //new StreetProducer().createTopic(topic);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,
                UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());


        builder.setSpout("StreetInfo", new KafkaSpout(kafkaSpoutConfig),10);
        //parallelism hint: number of thread for node
        //builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("StreetInfo");
        //builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");
        builder.setBolt("split", new SplitterBolt(),10).shuffleGrouping("StreetInfo");
        builder.setBolt("mean", new AreaBolt(),10).shuffleGrouping("split");


        Config conf = new Config();
        conf.setDebug(false);




        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        System.err.print("Submitted topology " + topic + "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
        Utils.sleep(1000000);
        cluster.killTopology("test");
        cluster.shutdown();*/

    }

}

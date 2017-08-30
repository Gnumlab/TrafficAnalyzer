package com.bridgestone.storm;

import com.bridgestone.bolt.MeanCalculatorBolt;
import com.bridgestone.bolt.ProducerBolt;
import com.bridgestone.bolt.SplitterBolt;
import com.bridgestone.bolt.UpdateMeanStreetBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.UUID;

/**
 * Created by francesco on 23/07/17.
 */
public class StormMain {

    public static void main(String[] args) throws Exception {




        TopologyBuilder builder = new TopologyBuilder();
        String zkConnString = "localhost:2181";
        double x, y;
        x = 52.12;
        y = 41.34;
        String topic = Double.toString(x) + Double.toString(y);
        BrokerHosts hosts = new ZkHosts(zkConnString);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,
                UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());




        builder.setSpout("StreetInfo", new KafkaSpout(kafkaSpoutConfig),2);
        //parallelism hint: number of thread for node
        //builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("StreetInfo");
        //builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");
        builder.setBolt("consumer", new SplitterBolt(),3).shuffleGrouping("StreetInfo");
        builder.setBolt("mean", new MeanCalculatorBolt(),3).shuffleGrouping("consumer");
        builder.setBolt("street", new UpdateMeanStreetBolt(),3).fieldsGrouping("mean", new Fields("edge"));
        builder.setBolt("producer", new ProducerBolt(),3).fieldsGrouping("street", new Fields("edge"));



        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            System.err.print("Submitted topology\n\n\n\n\n\n\n\n\n\n\n\n\n");
            Utils.sleep(10000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}


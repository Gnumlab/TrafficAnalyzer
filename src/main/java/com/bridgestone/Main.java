package com.bridgestone;

import com.bridgestone.bolt.*;
import com.bridgestone.kafka.KafkaTopicCreator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.UUID;

/**
 * Created by francesco on 18/07/17.
 */
public class Main {

    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();
        String zkConnString = "54.93.238.46";
        /*double x, y;
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


        builder.setSpout("StreetInfo", new KafkaSpout(kafkaSpoutConfig),10);
        //parallelism hint: number of thread for node
        //builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("StreetInfo");
        //builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");
        builder.setBolt("split", new SplitterBolt(),10).shuffleGrouping("StreetInfo");
        builder.setBolt("mean", new AreaBolt(),10).shuffleGrouping("split");*/
        double endingX = 12.7;
        double endingY = 42.00;
        for(double x = 12.3; x <= endingX; x = x + 0.1){
            for( double y = 41.75; y<= endingY; y = y + 0.1){
                String topic = Double.toString(x) + Double.toString(y);
                try {
                    KafkaTopicCreator.createTopic(zkConnString, topic);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                BrokerHosts hosts = new ZkHosts(zkConnString.concat(":2181"));

                SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,
                        UUID.randomUUID().toString());
                kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
                kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
                kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
                kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
                builder.setSpout("StreetInfo".concat(topic), new KafkaSpout(kafkaSpoutConfig),2);
                builder.setBolt("split".concat(topic), new SplitterAreaBolt(),3).shuffleGrouping("StreetInfo".concat(topic));
                builder.setBolt("mean".concat(topic), new AreaBolt(),3).fieldsGrouping("split".concat(topic), new Fields("fieldgroupkey"));
                //builder.setBolt("mean", new AreaBolt(),10).fieldsGrouping("split".concat(topic), new Fields("fieldgroupkey"));
            }
        }

        Config conf = new Config();
        conf.setDebug(false);




        /*LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        System.err.print("Submitted topology " + topic + "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
        Utils.sleep(1000000);
        cluster.killTopology("test");
        cluster.shutdown();*/
        conf.setNumWorkers(20);
        conf.setMaxSpoutPending(5000);
        try {
            StormSubmitter.submitTopology("test", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }

    }

}


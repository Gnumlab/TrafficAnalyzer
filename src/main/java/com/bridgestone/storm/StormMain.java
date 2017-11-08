package com.bridgestone.storm;

import com.bridgestone.bolt.MeanCalculatorBolt;
import com.bridgestone.bolt.ProducerBolt;
import com.bridgestone.bolt.SplitterBolt;
import com.bridgestone.bolt.UpdateMeanStreetBolt;
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
 * Created by francesco on 23/07/17.
 */
public class StormMain {

    public static void main(String[] args) throws Exception {



        TopologyBuilder builder = new TopologyBuilder();
        String zkConnString = "35.158.214.67";//"54.93.238.46"; 35.158.214.67
        /*double x, y;
        x = 52.12;
        y = 41.34;*/
        double endingX = 12.4;
        double endingY = 41.86;
        for(double x = 12.3; x <= endingX; x = x + 0.1){
            for( double y = 41.75; y<= endingY; y = y + 0.1){
                String topic = Double.toString(x) + Double.toString(y);
                KafkaTopicCreator.createTopic(zkConnString, topic);
                BrokerHosts hosts = new ZkHosts(zkConnString.concat(":2181"));

                SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,
                        UUID.randomUUID().toString());
                kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
                kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
                kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
                kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
                builder.setSpout("StreetInfo".concat(topic), new KafkaSpout(kafkaSpoutConfig),2);
                builder.setBolt("consumer".concat(topic), new SplitterBolt(),3).shuffleGrouping("StreetInfo".concat(topic));
                builder.setBolt("mean".concat(topic), new MeanCalculatorBolt(),3).shuffleGrouping("consumer".concat(topic));
                builder.setBolt("street".concat(topic), new UpdateMeanStreetBolt(),3).fieldsGrouping("mean".concat(topic), new Fields("edge"));
                builder.setBolt("producer".concat(topic), new ProducerBolt(),3).fieldsGrouping("street".concat(topic), new Fields("street"));
            }
        }
        /*String topic = Double.toString(x) + Double.toString(y);
        BrokerHosts hosts = new ZkHosts(zkConnString);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,
                UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());




        builder.setSpout("StreetInfo", new KafkaSpout(kafkaSpoutConfig),2);
        builder.setBolt("consumer", new SplitterBolt(),3).shuffleGrouping("StreetInfo");
        builder.setBolt("mean", new MeanCalculatorBolt(),3).shuffleGrouping("consumer");
        builder.setBolt("street", new UpdateMeanStreetBolt(),3).fieldsGrouping("mean", new Fields("edge"));
        builder.setBolt("producer", new ProducerBolt(),3).fieldsGrouping("street", new Fields("street"));

*/

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setNumWorkers(10);
            conf.setMaxSpoutPending(5000);
            try {
                StormSubmitter.submitTopology("test-finale", conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
            /*LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test-finale", conf, builder.createTopology());
            System.err.print("Submitted topology\n\n\n\n\n\n\n\n\n\n\n\n\n");
            Utils.sleep(10000000);
            cluster.killTopology("test");
            cluster.shutdown();*/
        }
    }
}


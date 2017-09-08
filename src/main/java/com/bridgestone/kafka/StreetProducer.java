package com.bridgestone.kafka;

import java.util.Properties;

import kafka.admin.AdminClient;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * Created by balmung on 30/08/17.
 */
public class StreetProducer {

    private Producer<String, String> producer;

    public StreetProducer(){

       // create instance for properties to access producer configs
       Properties props = new Properties();

       //Assign localhost id
       props.put("bootstrap.servers", "localhost:9092");

       //Set acknowledgements for producer requests.
       props.put("acks", "all");

       //If the request fails, the producer can automatically retry,
       props.put("retries", 0);

       //Specify buffer size in config
       props.put("batch.size", 16384);

       //Reduce the no of requests less than 0
       props.put("linger.ms", 1);

       //The buffer.memory controls the total amount of memory available to the producer for buffering.
       props.put("buffer.memory", 33554432);

       props.put("key.serializer",
               "org.apache.kafka.common.serialization.StringSerializer");

       props.put("value.serializer",
               "org.apache.kafka.common.serialization.StringSerializer");

       producer = new KafkaProducer<>(props);
    }

    public void send(String topicName, Double speed){


        this.createTopic(topicName);
        producer.send(new ProducerRecord<>(topicName, speed.toString()));


    }

    public void close(){
        this.producer.close();
    }


    private void createTopic(String topicName){

        ZkClient zkClient = new ZkClient("localhost:2181", 100000, 100000, ZKStringSerializer$.MODULE$);

        // Security for Kafka was added in Kafka 0.9.0.0
        boolean isSecureKafkaCluster = false;

        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection("localhost:2181"), isSecureKafkaCluster);
        AdminUtils.createTopic(zkUtils, topicName, 10, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
        zkClient.close();



        /*ZkClient zkClient = new ZkClient("localhost:2181", 100000, 100000, ZKStringSerializer$.MODULE$);
        if(!AdminUtils.topicExists(zkClient, topicName)) {
            AdminUtils.createTopic(zkClient, topicName, 10, 1, new Properties());
        }

        zkClient.close();*/

        /*ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        try {
            String zookeeperHosts = "192.168.20.1:2181"; // If multiple zookeeper then -> String zookeeperHosts = "192.168.20.1:2181,192.168.20.2:2181";
            int sessionTimeOutInMs = 15 * 1000; // 15 secs
            int connectionTimeOutInMs = 10 * 1000; // 10 secs

            zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);


            int noOfPartitions = 2;
            int noOfReplication = 3;
            Properties topicConfiguration = new Properties();

            AdminUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication, topicConfiguration);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }*/
    }


    public static void main(String args[]){
        StreetProducer streetProducer = new StreetProducer();
        streetProducer.createTopic("sacddasdasfcsadf");
    }
}

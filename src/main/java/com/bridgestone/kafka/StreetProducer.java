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
    private String zookeeperHost;

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
       zookeeperHost = "localhost";
    }

    public StreetProducer(String host){

        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", host + ":9092");

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
        zookeeperHost = host;
    }


    public void send(String topicName, Double speed){


        this.createTopic(topicName);
        producer.send(new ProducerRecord<>(topicName, speed.toString()));


    }

    public void close(){
        this.producer.close();
    }


    private void createTopic(String topicName) {
        try {
            KafkaTopicCreator.createTopic(this.zookeeperHost, topicName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]){
        StreetProducer streetProducer = new StreetProducer();
        streetProducer.createTopic("sacddasdasfcsadf");
    }
}

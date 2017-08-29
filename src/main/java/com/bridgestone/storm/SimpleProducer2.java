package com.bridgestone.storm;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.utils.Utils;

import java.util.Properties;

/**
 * Created by francesco on 24/07/17.
 */
public class SimpleProducer2 {

    public static void main(String[] args) throws Exception {


        //Assign topicName to string variable
        double x, y;
        x = 52.12;
        y = 41.34;
        String topicName = Double.toString(x) + Double.toString(y); //topic name: coordinates of the master section

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

        Producer<String, String> producer = new KafkaProducer<String, String>(props);


        String data = "[";

        data = data + jsonFormat(52.12 , 41.34 , 52.12 + 50 + 1, 41.34 +50 +1, 0);
        data = data + "," + jsonFormat(52.12 , 41.34 , 152.12 + 50 + 1, 41.34 +50 +1, 0);

        for (int i = 0; i <= 1000; i++) {
            //Utils.sleep(1);

            data = data + "," + jsonFormat(52.12 + i , 41.34 + i , 52.12 + i + 1, 41.34 +i +1, i);
            //System.out.println("Message sent successfully");
        }


        data = data + "]";
        System.err.println(data);
        producer.send(new ProducerRecord<String, String>(topicName, data));
        producer.close();
    }

    private static String jsonFormat(double x1, double y1, double x2, double y2, int speed){
        return "{\"x1\":" + Double.toString(x1) + ",\"y1\":" + Double.toString(y1)
                + ",\"x2\":" + Double.toString(x2)+ ",\"y2\":" + Double.toString(y2) +
        ",\"speed\":" + Integer.toString(speed) + "}";
    }
}

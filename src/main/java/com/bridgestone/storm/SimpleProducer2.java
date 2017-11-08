package com.bridgestone.storm;

import com.bridgestone.kafka.KafkaTopicCreator;
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
       /* double x, y;
        x = 52.12;
        y = 41.34;
        String topicName = Double.toString(x) + Double.toString(y); //topic name: coordinates of the master section
*/
        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "54.93.238.46:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 6);

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


        Double xTopic = 12.3;
        Double yTopic = 41.75;
        String topicName = xTopic.toString() + yTopic.toString();
        boolean first = true;
        for (int k = 0; k < 10; k++) {
            String data = "[";
            double x1 = 12.3111 + k*0.0010;
            double y1 = 41.8111 - k*0.0010;
            double x2 = 12.3112 + k*0.0010;
            double y2 = 41.8111 + k*0.0010;
            for (int j = 0; j < 10; j++, x1 = x2, x2 = x2 + 0.0001) {

                //data = data + jsonFormat(52.12, 41.34, 52.12 + 50 + 1, 41.34 + 50 + 1, 0);
                //data = data + "," + jsonFormat(52.12, 41.34, 152.12 + 50 + 1, 41.34 + 50 + 1, 0);

                for (int i = 0; i < 50; i++, y1 = y2, y2 = y2 + 0.0001) {
                    //Utils.sleep(1)
                    if (first) {
                        data = data + jsonFormat(x1, y1, x2, y2, i);
                        first = false;
                    } else {
                        data = data + "," + jsonFormat(x1, y1, x2, y2, i);
                        //System.out.println("Message sent successfully");
                    }
                    if(!topicName.equals(getTopicName(x2, y2))){
                        break;
                    }
                }

                    if(!topicName.equals(getTopicName(x2, y2))) {
                        data = data + "]";
                        System.err.println(data);
                        String zookeeperHost = "54.93.238.46";
                        KafkaTopicCreator.createTopic(zookeeperHost, topicName);
                        producer.send(new ProducerRecord<String, String>(topicName, data));
                        first = true;
                        topicName = getTopicName(x2, y2);
                        break;
                    }

                }
        }

        producer.close();
    }

    private static String jsonFormat(double x1, double y1, double x2, double y2, int speed){
        return "{\"x1\":" + Double.toString(x1) + ",\"y1\":" + Double.toString(y1)
                + ",\"x2\":" + Double.toString(x2)+ ",\"y2\":" + Double.toString(y2) +
        ",\"speed\":" + Integer.toString(speed) + "}";
    }

    private static String getTopicName(Double x, Double y){
        String first = x.toString();
        String second = y.toString();
        if(first.length() >= 5) {
            first = first.substring(0, 5);
        }
        if(second.length() >= 5)
            second= second.substring(0,5);
        return first + second;
    }
}

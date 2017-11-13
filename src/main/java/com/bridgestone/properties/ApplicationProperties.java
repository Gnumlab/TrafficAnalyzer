package com.bridgestone.properties;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
/**
 * Created by francesco on 13/11/17.
 */
public class ApplicationProperties {

    private static String kafkaAddress = null;
    private static String elasticSearchAddress = null;
    private static String redisAddress = null;
    private static Integer redisFirstPort = null;
    private static Integer redisSecondPort = null;
    private static Integer redisThirdPort = null;
    /** N.B: it is only needed the first port: the minimum redis cluster needs three nodes, but even using
     * three nodes redis client is able to discover all the components of the cluster*/

    public static void loadProperties(){

        Properties prop = new Properties();
        InputStream input = null;

        try {

            String filename = "config.properties";
            input = new FileInputStream("config.properties");

            if(input==null){
                System.out.println("Sorry, unable to find " + filename + "; System will exit!");
                System.exit(0);
            }

            //load a properties file from class path, inside static method
            prop.load(input);

            //get the property value and sets them
            ApplicationProperties.kafkaAddress = prop.getProperty("kafkaAddress");
            ApplicationProperties.elasticSearchAddress = prop.getProperty("elasticSearchAddress");
            ApplicationProperties.redisAddress = prop.getProperty("redisAddress");
            ApplicationProperties.redisFirstPort = Integer.getInteger(prop.getProperty("redisFirstPort"));
            ApplicationProperties.redisSecondPort = Integer.getInteger(prop.getProperty("redisSecondPort"));
            ApplicationProperties.redisThirdPort = Integer.getInteger(prop.getProperty("redisThirdPort"));

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally{
            if(input!=null){
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();

                }
            }
        }

    }

    public static String getKafkaAddress() {
        return kafkaAddress;
    }

    public static String getElasticSearchAddress() {
        return elasticSearchAddress;
    }

    public static String getRedisAddress() {
        return redisAddress;
    }

    public static String getRedisFirstPort() {
        return redisFirstPort.toString();
    }

    public static String getRedisSecondPort() {
        return redisSecondPort.toString();
    }

    public static String getRedisThirdPort() {
        return redisThirdPort.toString();
    }
}

package com.bridgestone.elasticsearch;

import com.bridgestone.utils.random.Rngs;
import com.bridgestone.utils.random.Rvgs;
import org.elasticsearch.action.get.GetResponse;

import java.io.IOException;

import static jodd.util.ThreadUtil.sleep;

/**
 * Created by balmung on 08/09/17.
 */
public class TestElasticsearch {

    public static void main(String args[]) {
        Rvgs rvgs = new Rvgs(new Rngs());
        int sleepTime = 1;
        int poissonMean = 10;
        int count = 50;
        for(;;) {
            long requests = rvgs.poisson(poissonMean);
            for(int i = 0; i < requests; i++) {
                CloudClient cloudClient = new CloudClient();
                try {
                    GetResponse response = cloudClient.get("search-traffic-analyzer-indexes-faaztbzp3bx3q7hitgjkb44wwi.eu-central-1.es.amazonaws.com", 9300,
                            "streetindex", "streetinfo", "2");
                    System.err.println(response.getSource());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (count == 50)
                poissonMean *= 2;
            if (count == 100)
                poissonMean *= 2;
            System.err.println(requests);
            sleep(sleepTime);
        }

    }
}

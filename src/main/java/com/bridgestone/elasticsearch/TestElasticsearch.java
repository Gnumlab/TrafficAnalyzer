package com.bridgestone.elasticsearch;

import com.bridgestone.utils.random.Rngs;
import com.bridgestone.utils.random.Rvgs;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;

import static jodd.util.ThreadUtil.sleep;

/**
 * Created by balmung on 08/09/17.
 */
public class TestElasticsearch {
    public static void main(String args[]) {
        Rvgs rvgs = new Rvgs(new Rngs());

        int sleepTime = 200;
        int poissonMean = 10;
        int count = 0;
        for(;;) {
            count++;
            long requests = rvgs.poisson(poissonMean);
            for(int i = 0; i < requests; i++) {
                CloudClient cloudClient = new CloudClient();
                try {
                    GetResponse response = cloudClient.getStreet("search-my-elastic-domain-dioeomsyqpdv2m5yzqghk5wqrq.eu-central-1.es.amazonaws.com", 9300,
                            "streetindex", "streetinfo", Integer.toString(i%25));
                    System.err.println(response.getSource() + "\t" + response.getIndex() +
                            "\t" + response.getType() + "\t" + response.getId());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            count++;
            if (count%50 == 0)
                poissonMean *= 2;
            if (count == 400)
                break;
            System.err.println(requests);
            sleep(sleepTime);
        }


    }

}

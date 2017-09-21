package com.bridgestone.elasticsearch;

import javafx.scene.NodeBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.threadpool.ThreadPool.Names.INDEX;

/**
 * Created by balmung on 08/09/17.
 */
public class TestElasticsearch {

    public static void main(String args[]) {
        CloudClient cloudClient = new CloudClient();
        try {
            GetResponse response = cloudClient.get("search-traffic-analyzer-indexes-faaztbzp3bx3q7hitgjkb44wwi.eu-central-1.es.amazonaws.com", 9300,
            "streetindex", "streetinfo", "2");
            System.err.println(response.getSource());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

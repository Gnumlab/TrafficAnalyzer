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
import org.springframework.expression.spel.ast.Indexer;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.threadpool.ThreadPool.Names.INDEX;

/**
 * Created by balmung on 08/09/17.
 */
public class TestElasticsearch {

    public static void main(String args[]) {
        try {
            TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
            GetResponse response = client.prepareGet("streetindex", "streetinfo", "2").execute().actionGet();
            System.err.println(response.getSource().get("speed") + " " + response.getSource().get("edges"));
            client.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}

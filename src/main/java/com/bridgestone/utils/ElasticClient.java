package com.bridgestone.utils;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Created by francesco on 15/09/17.
 */
public interface ElasticClient {

    public UpdateResponse updateSpeedStreet(String address, int port, String index,
                                            String type, String streetKey, Double speed) throws
            IOException, ExecutionException, InterruptedException;

    public IndexResponse createIndexes(String address, int port, String index, String type, String edges, String streetKey) throws IOException, ExecutionException, InterruptedException ;
}

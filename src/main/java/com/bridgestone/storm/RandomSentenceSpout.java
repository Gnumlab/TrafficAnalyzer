package com.bridgestone.storm;

import org.apache.storm.spout.SpoutOutputCollector;
        import org.apache.storm.task.TopologyContext;
        import org.apache.storm.topology.OutputFieldsDeclarer;
        import org.apache.storm.topology.base.BaseRichSpout;
        import org.apache.storm.tuple.Fields;
        import org.apache.storm.tuple.Values;
        import org.apache.storm.utils.Utils;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

        import java.text.SimpleDateFormat;
        import java.util.Date;
        import java.util.Map;
        import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(RandomSentenceSpout.class);

    SpoutOutputCollector _collector;
    Random _rand;


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        System.err.print("Opening!!!!\n\n\n\n\n\n\n\n");
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        String[] words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
        String word = words[_rand.nextInt(words.length)];
        System.err.print(word + "\n\n\n\n\n\n\n\n\n\n\n\n");
        _collector.emit(new Values(word));
    }

    protected String sentence(String input) {
        return input;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
        System.err.print("Failing!!!!!!!!!!\n\n\n\n\n\n\n\n\n\n\n\n");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}
package com.hortonworks.storm.analytics;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class TweetCountBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(TweetCountBolt.class);
    long tweetCount;
    
    @Override @SuppressWarnings("rawtypes")
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple tuple) {
        //Increment the counter and move on ..
        //String tweet = tuple.getString(0);
        //LOG.info(tweet);
        tweetCount++;
        //Display output every hundredth time 
        if(tweetCount%100 == 0){
            LOG.info("TWEET COUNTER....:" + tweetCount);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { 
    }

}

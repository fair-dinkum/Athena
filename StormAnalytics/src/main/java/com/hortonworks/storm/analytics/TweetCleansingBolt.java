package com.hortonworks.storm.analytics;

import java.util.Map;

import org.json.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class TweetCleansingBolt extends BaseRichBolt{
    
    private OutputCollector collector;
    
    @Override @SuppressWarnings("rawtypes")
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String tweetString = tuple.getString(0).trim();
        JSONObject tweet = new JSONObject(tweetString);
        
        // Parse and cleanse only en tweets
        if(tweet.has("lang") && tweet.get("lang").toString().trim().toLowerCase().equals("en")){
            String cleansedTweetString = tweet.get("text")
                    .toString()
                    .trim()
                    .replaceAll("[^A-Za-z ]", "") //Keep chars and spaces
                    .replaceAll(" +", " ") // Replace multiple spaces with single space 
                    .toLowerCase();
            collector.emit(new Values(cleansedTweetString));
        }   
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { 
        declarer.declare(new Fields("cleansedTweetString"));
    }

}

package com.hortonworks.storm.analytics;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class TweetGroupedByLanguageBolt extends BaseRichBolt{
    private static final Logger LOG = Logger.getLogger(TweetGroupedByLanguageBolt.class);
    Map<String, Integer> tweetByLanguageMap;
    private long displayCounter;

    @Override @SuppressWarnings("rawtypes")
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.tweetByLanguageMap = new LinkedHashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple) {
        String tweetString = tuple.getString(0).trim();
        JSONObject tweet = new JSONObject(tweetString);

        if(tweet.has("lang") && !tweet.get("lang").toString().trim().isEmpty()){
            String lang = tweet.get("lang").toString().trim();
            if(!tweetByLanguageMap.containsKey(lang)){
                tweetByLanguageMap.put(lang, 1);
            } else {
                Integer langCount = tweetByLanguageMap.get(lang) + 1;
                tweetByLanguageMap.put(lang, langCount);
            }
            displayCounter++;
        }
        //Display updated output every thousandth tweet 
        if(displayCounter%1000 == 0){
            LOG.info("Tweets Grouped by Language Over the last " + displayCounter + " Tweets....."  );
            for(Map.Entry<String, Integer> language:tweetByLanguageMap.entrySet()){
                LOG.info(language.getKey() + " : " + language.getValue());
            }

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { 
    }

}

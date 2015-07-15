package com.hortonworks.storm.analytics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class SentimentAnalyzerBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(SentimentAnalyzerBolt.class);
    private Map<String,Integer> bagOfWords = new HashMap<String,Integer>();
    private enum sentiment{POSITIVE,NEGATIVE,NEUTRAL};
    String afinnFile="/AFINN.data";

    @Override @SuppressWarnings("rawtypes")
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        //Load AFFIN data into a HashMap
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                this.getClass().getResourceAsStream(afinnFile)));
            String line="";
            while((line = reader.readLine()) != null){
                String[] kv = line.split("\t");
                bagOfWords.put(kv[0], Integer.parseInt(kv[1]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String tweet = tuple.getString(0);
        String[] tweetWords = tweet.split("\\s+");

        //Calculate sentiment score for tweet
        float sentimentScore = 0;
        float sentimentStrength = 0;
        for(String word : tweetWords){
            if (bagOfWords.containsKey(word)){
                sentimentScore+=bagOfWords.get(word);
            }
        }

        //sentiment strength based on the input text
        //+ve values are +ve valence, -ve value are -ve valence
        sentimentStrength=(float) (sentimentScore/Math.sqrt(tweetWords.length));

        if(sentimentStrength>0){
            LOG.info(tweet + " : " + sentiment.POSITIVE );
        } else if(sentimentStrength<0){
            LOG.info(tweet + " : " + sentiment.NEGATIVE );
        }else if(sentimentStrength==0){
            LOG.info(tweet + " : " + sentiment.NEUTRAL );
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { 
    }

}

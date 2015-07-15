package com.hortonworks.kafka;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.json.JSONObject;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
    
    public static void run(Properties properties) throws InterruptedException {
        
        //Kafka producer properties
        Properties props = new Properties();
        props.put("metadata.broker.list", properties.getProperty("kafkaBrokerlist"));
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        
        //Twitter SampleEndpoint
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(15);
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.stallWarnings(false);
        
        //Use trackTerms if you want to track terms - Ex:
        //endpoint.trackTerms(Lists.newArrayList("potus", "obama"));

        // Authenticate
        Authentication auth = new OAuth1(
            properties.getProperty("consumerKey"), 
            properties.getProperty("consumerSecret"), 
            properties.getProperty("accessToken"), 
            properties.getProperty("accessTokenSecret"));
        
        Client client = new ClientBuilder()
                .name("HortonworksTwitterStreaming")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Establish a connection
        client.connect();
        
        while (!client.isDone()) {
            String tweetString = queue.take().toString().trim();
            JSONObject tweet = new JSONObject(tweetString);
            
            //Only store valid tweets
            if(tweet.has("text") && !tweet.get("text").toString().isEmpty()){
                KeyedMessage<String, String> data = new KeyedMessage<String, String>(
                        properties.getProperty("kafkaTopic"), 
                        tweetString);
                producer.send(data);
                //System.out.println(tweet.get("text").toString().trim());
            }
        }
      }
    
    public static void main(String[] args) {
        final Properties properties = new Properties();
        
        try {
            properties.load(TwitterProducer.class.getResourceAsStream("/producer.conf"));
            TwitterProducer.run(properties);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        
    }

}

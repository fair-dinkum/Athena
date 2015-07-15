package com.hortonworks.storm.analytics;

import java.io.IOException;
import java.util.Properties;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

public class TwitterAnalyticsTopology {

    public void configureKafkaHdfsBolt(TopologyBuilder topology, String spout, String hdfsUri){

        // Write from buffer to HDFS every 100 tuples
        // Rotate data file every ~100 HDFS block
        // Store file under /archive and Use storm-generated file names
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("\n");
        SyncPolicy syncPolicy = new CountSyncPolicy(100);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(12800.0f, Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/archive");

        // Instantiate HDFS Bolt
        HdfsBolt bolt = new HdfsBolt().withFsUrl(hdfsUri)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
        topology.setBolt("HdfsBolt", bolt, 2).shuffleGrouping(spout);
    }

    public void configureKafkaSpout(TopologyBuilder topology){
        BrokerHosts brokerHosts = new ZkHosts("10.10.11.99:2181,10.10.11.102:2181,10.10.11.103:2181");
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "tweets", "", "TwitterAnalyticsTopology");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        
        // Instantiate Kafka Spout
        topology.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 1);
    }

    public void buildAndDeployTopology(Properties properties){
        Config conf = new Config();
        TopologyBuilder topology = new TopologyBuilder();

        conf.setNumWorkers(Integer.parseInt(properties.getProperty("NumWorkers")));
        conf.setMaxSpoutPending(Integer.parseInt(properties.getProperty("MaxSpoutPending")));

        configureKafkaSpout(topology);
        configureKafkaHdfsBolt(topology,"KafkaSpout", properties.getProperty("hdfsUri"));
        topology.setBolt("TweetCountBolt", new TweetCountBolt(), 1).shuffleGrouping("KafkaSpout");
        topology.setBolt("TweetGroupedByLanguageBolt", new TweetGroupedByLanguageBolt(), 1).shuffleGrouping("KafkaSpout");
        topology.setBolt("TweetCleansingBolt", new TweetCleansingBolt(), 2).shuffleGrouping("KafkaSpout");
        topology.setBolt("SentimentAnalyzerBolt", new SentimentAnalyzerBolt(), 1).shuffleGrouping("TweetCleansingBolt");

        try {
            StormSubmitter.submitTopology("RealTimeTwitterAnalytics", conf, topology.createTopology());
        } catch (AlreadyAliveException | InvalidTopologyException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        final Properties properties = new Properties();
        
        try {
            properties.load(TwitterAnalyticsTopology.class.getResourceAsStream("/topology.conf"));
            TwitterAnalyticsTopology twittterAnalyticsTopology = new  TwitterAnalyticsTopology();
            twittterAnalyticsTopology.buildAndDeployTopology(properties);
        } catch (IOException e) {
            e.printStackTrace();
        }
        

    }

}

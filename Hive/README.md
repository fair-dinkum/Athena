#HIVE

## Store the twitter data sample_twitter_data.txt in HDFS

```
$ dos2unix sample_twitter_data.txt
$ hadoop fs -mkdir /user/hive/data
$ hadoop fs -copyFromLocal -f ~/data/sample_twitter_data.txt /user/hive/data/
```

## Create Hive table and load data

```
CREATE EXTERNAL TABLE tweets ( 
   user STRUCT < userlocation:STRING, id:BIGINT, name:STRING, screenname:STRING, geoenabled:STRING >, 
   tweetmessage STRING,
   createddate STRING,
   geoLocation STRUCT < latitude:FLOAT, longitude:FLOAT >
    ) 
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' 
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/data/sample_twitter_data.txt' OVERWRITE INTO TABLE tweets;
```

## All the tweets by the twitter user "Aimee_Cottle"

```
SELECT 
    tweetmessage 
FROM 
    tweets 
WHERE 
    user.screenname='Aimee_Cottle';
```
![Hive](https://github.com/fair-dinkum/Athena/blob/master/Hive/hive.png)



from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import json
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
from pyspark.sql import Row, SparkSession
from pyspark.streaming.kafka import KafkaUtils

from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer

def read_credentials():
    file_name = "CCGAcredentials.json"
    try:
        with open(file_name) as data_file:
            return json.load(data_file)
    except:
        print ("Cannot load "+data_file)
        return None


def getSparkSessionInstance(sparkConf):
	if ('sparkSessionSingletonInstance' not in globals()):
		globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()
	return globals()['sparkSessionSingletonInstance']


def tweetStream():
    ssc = StreamingContext(sc, 600)

    kvs = KafkaUtils.createDirectStream(ssc, ["test"], {"metadata.broker.list": "localhost:9092"})
    kvs.foreachRDD(getTweets)
    producer.flush()
    ssc.start()
    ssc.awaitTermination()


def getTweets(time, rdd):
    iterator = twitter_stream.statuses.sample()
    count = 0
    for tweet in iterator:
        if 'extended_tweet' in tweet:

            producer.send('savedata', bytes(json.dumps(tweet, indent=6), "ascii"))
            count+=1
            if(count>=20000):
                break

if __name__ == "__main__":
    sc = SparkContext(appName="BDproject2 Tweet Stream")
    credentials = read_credentials()
    oauth = OAuth(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'], credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    twitter_stream = TwitterStream(auth=oauth)
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    tweetStream()

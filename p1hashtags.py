from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row
import csv

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionInstance' not in globals()):
        globals()['sparkSessionInstance'] = SparkSession.builder.config(conf=sparkConf) \
                                            .enableHiveSupport().getOrCreate()
    return globals()['sparkSessionInstance']

def outputQuery():
    spark = getSparkSessionInstance(sc.getConf())
    df = spark.sql("use default")

    #Query to extract top hashtags from DF
    df = spark.sql("select hashtag, sum(total) as suma from hashtagTable where timestamp between cast('2018-12-07 14:00:00' as timestamp)- INTERVAL 1 HOUR and cast('2018-12-07 14:00:00' as timestamp) group by hashtag order by suma desc limit 10")
    df.show()
    #write to CSV
    df.repartition(1).write.csv("/data/BDp1.csv")

if _name_ == "_main_":
    sc = SparkContext(appName="p1")
    outputQuery()

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
    df = spark.sql("select sname, sum(total) as count from screennameTable where timestamp between cast('2018-12-07 14:00:00' as timestamp)- INTERVAL 12 HOUR and cast('2018-12-07 14:00:00' as timestamp) group by sname order by suma desc limit 10")
    df.show()
    df.repartition(1).write.csv("/data/DBp3.csv")

if _name_ == "_main_":
    sc = SparkContext(appName="p3")
    outputQuery()

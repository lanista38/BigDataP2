from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import json
from pyspark.sql import Row, SparkSession
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar'

def getSparkSessionInstance(sparkConf):
	if ('sparkSessionSingletonInstance' not in globals()):
		globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()
	return globals()['sparkSessionSingletonInstance']

# Read tweets from stream
def readtweets():
    ssc = StreamingContext(sc, 300)
    kvs = KafkaUtils.createDirectStream(ssc, ["savedata"], {"metadata.broker.list": "localhost:9092"})
    kvs.foreachRDD(dfCreate)
    ssc.start()
    ssc.awaitTermination()


def dfCreate(time, rdd):
    spark = getSparkSessionInstance(rdd.context.getConf())
    rdd = rdd.map(lambda x: json.loads(x[1]))
    if rdd.count() > 0:
        #Hashtags data frame creation
        data = rdd.filter(lambda x: len(x["extended_tweet"]["entities"]["hashtags"]) > 0)
        if (data.count() > 0):
            hashtags = data.map(lambda x: (x["extended_tweet"]['entities']['hashtags']))
            hashtags = hashtags.map(lambda x: x[0])
            hashtags = hashtags.map(lambda x: x['text'])
            df = spark.createDataFrame(hashtags.map(lambda x: Row(hashtag=x , timestamp=time)))
            df.createOrReplaceTempView("hashtags")
            df = spark.sql("create database if not exists default")
            df = spark.sql("use default")
            df = spark.sql("select hashtag, count(*) as total, timestamp from hashtags group by hashtag, timestamp order by total desc limit 5")
            df.write.mode("append").saveAsTable("hashtagTable")
        else:
            print("No hashtags to be inserted")

        #Keywords data frame creation with stop words
        data = rdd.map(lambda x: x["extended_tweet"]["full_text"])
        data = data.map(lambda x: x.split(" ")).flatMap(lambda x: x).map(lambda x: x.upper())
        data = data.filter(lambda x: x.startswith(('@','#','HTTP','"', '&')) == False ).filter(lambda x: x.endswith(('"')) == False).filter(lambda x: len(x) > 2 )
        data = data.filter(lambda x: (x != "A'S" and x != "ABLE" and x != "ABOUT" and x != "ABOVE" and x != "ACCORDING" and x != "ACCORDINGLY" and x != "ACROSS" and x != "ACTUALLY" and x != "AFTER" and x != "AFTERWARDS" and x != "AGAIN" and x != "AGAINST" and x != "AIN'T" and x != "ALL" and x != "ALLOW" and x != "ALLOWS" and x != "ALMOST" and x != "ALONE" and x != "ALONG" and x != "ALREADY" and x != "ALSO" and x != "ALTHOUGH" and x != "ALWAYS" and x != "AM" and x != "AMONG" and x != "AMONGST" and x != "AN" and x != "AND" and x != "ANOTHER" and x != "ANY" and x != "ANYBODY" and x != "ANYHOW" and x != "ANYONE" and x != "ANYTHING" and x != "ANYWAY" and x != "ANYWAYS" and x != "ANYWHERE" and x != "APART" and x != "APPEAR" and x != "APPRECIATE" and x != "APPROPRIATE" and x != "ARE" and x != "AREN'T" and x != "AROUND" and x != "AS" and x != "ASIDE" and x != "ASK" and x != "ASKING" and x != "ASSOCIATED" and x != "AT" and x != "AVAILABLE" and x != "AWAY" and x != "AWFULLY" and x != "B" and x != "BE" and x != "BECAME" and x != "BECAUSE" and x != "BECOME" and x != "BECOMES" and x != "BECOMING" and x != "BEEN" and x != "BEFORE" and x != "BEFOREHAND" and x != "BEHIND" and x != "BEING" and x != "BELIEVE" and x != "BELOW" and x != "BESIDE" and x != "BESIDES" and x != "BEST" and x != "BETTER" and x != "BETWEEN" and x != "BEYOND" and x != "BOTH" and x != "BRIEF" and x != "BUT" and x != "BY" and x != "C" and x != "C'MON" and x != "C'S" and x != "CAME" and x != "CAN" and x != "CAN'T" and x != "CANNOT" and x != "CANT" and x != "CAUSE" and x != "CAUSES" and x != "CERTAIN" and x != "CERTAINLY" and x != "CHANGES" and x != "CLEARLY" and x != "CO" and x != "COM" and x != "COME" and x != "COMES" and x != "CONCERNING" and x != "CONSEQUENTLY" and x != "CONSIDER" and x != "CONSIDERING" and x != "CONTAIN" and x != "CONTAINING" and x != "CONTAINS" and x != "CORRESPONDING" and x != "COULD" and x != "COULDN'T" and x != "COURSE" and x != "CURRENTLY" and x != "D" and x != "DEFINITELY" and x != "DESCRIBED" and x != "DESPITE" and x != "DID" and x != "DIDN'T" and x != "DIFFERENT" and x != "DO" and x != "DOES" and x != "DOESN'T" and x != "DOING" and x != "DON'T" and x != "DONE" and x != "DOWN" and x != "DOWNWARDS" and x != "DURING" and x != "E" and x != "EACH" and x != "EDU" and x != "EG" and x != "EIGHT" and x != "EITHER" and x != "ELSE" and x != "ELSEWHERE" and x != "ENOUGH" and x != "ENTIRELY" and x != "ESPECIALLY" and x != "ET" and x != "ETC" and x != "EVEN" and x != "EVER" and x != "EVERY" and x != "EVERYBODY" and x != "EVERYONE" and x != "EVERYTHING" and x != "EVERYWHERE" and x != "EX" and x != "EXACTLY" and x != "EXAMPLE" and x != "EXCEPT" and x != "F" and x != "FAR" and x != "FEW" and x != "FIFTH" and x != "FIRST" and x != "FIVE" and x != "FOLLOWED" and x != "FOLLOWING" and x != "FOLLOWS" and x != "FOR" and x != "FORMER" and x != "FORMERLY" and x != "FORTH" and x != "FOUR" and x != "FROM" and x != "FURTHER" and x != "FURTHERMORE" and x != "G" and x != "GET" and x != "GETS" and x != "GETTING" and x != "GIVEN" and x != "GIVES" and x != "GO" and x != "GOES" and x != "GOING" and x != "GONE" and x != "GOT" and x != "GOTTEN" and x != "GREETINGS" and x != "H" and x != "HAD" and x != "HADN'T" and x != "HAPPENS" and x != "HARDLY" and x != "HAS" and x != "HASN'T" and x != "HAVE" and x != "HAVEN'T" and x != "HAVING" and x != "HE" and x != "HE'S" and x != "HELLO" and x != "HELP" and x != "HENCE" and x != "HER" and x != "HERE" and x != "HERE'S" and x != "HEREAFTER" and x != "HEREBY" and x != "HEREIN" and x != "HEREUPON" and x != "HERS" and x != "HERSELF" and x != "HI" and x != "HIM" and x != "HIMSELF" and x != "HIS" and x != "HITHER" and x != "HOPEFULLY" and x != "HOW" and x != "HOWBEIT" and x != "HOWEVER" and x != "I" and x != "I'D" and x != "I'LL" and x != "I'M" and x != "I'VE" and x != "IE" and x != "IF" and x != "IGNORED" and x != "IMMEDIATE" and x != "IN" and x != "INASMUCH" and x != "INC" and x != "INDEED" and x != "INDICATE" and x != "INDICATED" and x != "INDICATES" and x != "INNER" and x != "INSOFAR" and x != "INSTEAD" and x != "INTO" and x != "INWARD" and x != "IS" and x != "ISN'T" and x != "IT" and x != "IT'D" and x != "IT'LL" and x != "IT'S" and x != "ITS" and x != "ITSELF" and x != "J" and x != "JUST" and x != "K" and x != "KEEP" and x != "KEEPS" and x != "KEPT" and x != "KNOW" and x != "KNOWN" and x != "KNOWS" and x != "L" and x != "LAST" and x != "LATELY" and x != "LATER" and x != "LATTER" and x != "LATTERLY" and x != "LEAST" and x != "LESS" and x != "LEST" and x != "LET" and x != "LET'S" and x != "LIKE" and x != "LIKED" and x != "LIKELY" and x != "LITTLE" and x != "LOOK" and x != "LOOKING" and x != "LOOKS" and x != "LTD" and x != "M" and x != "MAINLY" and x != "MANY" and x != "MAY" and x != "MAYBE" and x != "ME" and x != "MEAN" and x != "MEANWHILE" and x != "MERELY" and x != "MIGHT" and x != "MORE" and x != "MOREOVER" and x != "MOST" and x != "MOSTLY" and x != "MUCH" and x != "MUST" and x != "MY" and x != "MYSELF" and x != "N" and x != "NAME" and x != "NAMELY" and x != "ND" and x != "NEAR" and x != "NEARLY" and x != "NECESSARY" and x != "NEED" and x != "NEEDS" and x != "NEITHER" and x != "NEVER" and x != "NEVERTHELESS" and x != "NEW" and x != "NEXT" and x != "NINE" and x != "NO" and x != "NOBODY" and x != "NON" and x != "NONE" and x != "NOONE" and x != "NOR" and x != "NORMALLY" and x != "NOT" and x != "NOTHING" and x != "NOVEL" and x != "NOW" and x != "NOWHERE" and x != "O" and x != "OBVIOUSLY" and x != "OF" and x != "OFF" and x != "OFTEN" and x != "OH" and x != "OK" and x != "OKAY" and x != "OLD" and x != "ON" and x != "ONCE" and x != "ONE" and x != "ONES" and x != "ONLY" and x != "ONTO" and x != "OR" and x != "OTHER" and x != "OTHERS" and x != "OTHERWISE" and x != "OUGHT" and x != "OUR" and x != "OURS" and x != "OURSELVES" and x != "OUT" and x != "OUTSIDE" and x != "OVER" and x != "OVERALL" and x != "OWN" and x != "P" and x != "PARTICULAR" and x != "PARTICULARLY" and x != "PER" and x != "PERHAPS" and x != "PLACED" and x != "PLEASE" and x != "PLUS" and x != "POSSIBLE" and x != "PRESUMABLY" and x != "PROBABLY" and x != "PROVIDES" and x != "Q" and x != "QUE" and x != "QUITE" and x != "QV" and x != "R" and x != "RATHER" and x != "RD" and x != "RE" and x != "REALLY" and x != "REASONABLY" and x != "REGARDING" and x != "REGARDLESS" and x != "REGARDS" and x != "RELATIVELY" and x != "RESPECTIVELY" and x != "RIGHT" and x != "S" and x != "SAID" and x != "SAME" and x != "SAW" and x != "SAY" and x != "SAYING" and x != "SAYS" and x != "SECOND" and x != "SECONDLY" and x != "SEE" and x != "SEEING" and x != "SEEM" and x != "SEEMED" and x != "SEEMING" and x != "SEEMS" and x != "SEEN" and x != "SELF" and x != "SELVES" and x != "SENSIBLE" and x != "SENT" and x != "SERIOUS" and x != "SERIOUSLY" and x != "SEVEN" and x != "SEVERAL" and x != "SHALL" and x != "SHE" and x != "SHOULD" and x != "SHOULDN'T" and x != "SINCE" and x != "SIX" and x != "SO" and x != "SOME" and x != "SOMEBODY" and x != "SOMEHOW" and x != "SOMEONE" and x != "SOMETHING" and x != "SOMETIME" and x != "SOMETIMES" and x != "SOMEWHAT" and x != "SOMEWHERE" and x != "SOON" and x != "SORRY" and x != "SPECIFIED" and x != "SPECIFY" and x != "SPECIFYING" and x != "STILL" and x != "SUB" and x != "SUCH" and x != "SUP" and x != "SURE" and x != "T" and x != "T'S" and x != "TAKE" and x != "TAKEN" and x != "TELL" and x != "TENDS" and x != "TH" and x != "THAN" and x != "THANK" and x != "THANKS" and x != "THANX" and x != "THAT" and x != "THAT'S" and x != "THATS" and x != "THE" and x != "THEIR" and x != "THEIRS" and x != "THEM" and x != "THEMSELVES" and x != "THEN" and x != "THENCE" and x != "THERE" and x != "THERE'S" and x != "THEREAFTER" and x != "THEREBY" and x != "THEREFORE" and x != "THEREIN" and x != "THERES" and x != "THEREUPON" and x != "THESE" and x != "THEY" and x != "THEY'D" and x != "THEY'LL" and x != "THEY'RE" and x != "THEY'VE" and x != "THINK" and x != "THIRD" and x != "THIS" and x != "THOROUGH" and x != "THOROUGHLY" and x != "THOSE" and x != "THOUGH" and x != "THREE" and x != "THROUGH" and x != "THROUGHOUT" and x != "THRU" and x != "THUS" and x != "TO" and x != "TOGETHER" and x != "TOO" and x != "TOOK" and x != "TOWARD" and x != "TOWARDS" and x != "TRIED" and x != "TRIES" and x != "TRULY" and x != "TRY" and x != "TRYING" and x != "TWICE" and x != "TWO" and x != "U" and x != "UN" and x != "UNDER" and x != "UNFORTUNATELY" and x != "UNLESS" and x != "UNLIKELY" and x != "UNTIL" and x != "UNTO" and x != "UP" and x != "UPON" and x != "US" and x != "USE" and x != "USED" and x != "USEFUL" and x != "USES" and x != "USING" and x != "USUALLY" and x != "UUCP" and x != "V" and x != "VALUE" and x != "VARIOUS" and x != "VERY" and x != "VIA" and x != "VIZ" and x != "VS" and x != "W" and x != "WANT" and x != "WANTS" and x != "WAS" and x != "WASN'T" and x != "WAY" and x != "WE" and x != "WE'D" and x != "WE'LL" and x != "WE'RE" and x != "WE'VE" and x != "WELCOME" and x != "WELL" and x != "WENT" and x != "WERE" and x != "WEREN'T" and x != "WHAT" and x != "WHAT'S" and x != "WHATEVER" and x != "WHEN" and x != "WHENCE" and x != "WHENEVER" and x != "WHERE" and x != "WHERE'S" and x != "WHEREAFTER" and x != "WHEREAS" and x != "WHEREBY" and x != "WHEREIN" and x != "WHEREUPON" and x != "WHEREVER" and x != "WHETHER" and x != "WHICH" and x != "WHILE" and x != "WHITHER" and x != "WHO" and x != "WHO'S" and x != "WHOEVER" and x != "WHOLE" and x != "WHOM" and x != "WHOSE" and x != "WHY" and x != "WILL" and x != "WILLING" and x != "WISH" and x != "WITH" and x != "WITHIN" and x != "WITHOUT" and x != "WON'T" and x != "WONDER" and x != "WOULD" and x != "WOULDN'T" and x != "X" and x != "Y" and x != "YES" and x != "YET" and x != "YOU" and x != "YOU'D" and x != "YOU'LL" and x != "YOU'RE" and x != "YOU'VE" and x != "YOUR" and x != "YOURS" and x != "YOURSELF" and x != "YOURSELVES" and x != "Z" and x != "ZERO"))

        df = spark.createDataFrame(data.map(lambda x: Row(word = x, timestamp = time)))
        df.createOrReplaceTempView("keywords")
        df = spark.sql("select word, count(*) as total, timestamp from keywords group by word, timestamp order by total desc limit 5")
        df.write.mode("append").saveAsTable("keywordTable")

        #Screen_Names data freame creation
        data = rdd.map(lambda x: x["user"]["screen_name"])
        df = spark.createDataFrame(data.map(lambda x: Row(name = x, timestamp = time)))
        df.createOrReplaceTempView("screen_names")
        df = spark.sql("select sname, count(*) as total, timestamp from screen_names group by sname, timestamp order by total desc limit 5")
        df.write.mode("append").saveAsTable("screennameTable")

        #Word Ocurrences data frame creation
        data = rdd.map(lambda x: x["extended_tweet"]["full_text"])
        data = data.map(lambda x: x.split(" ")).flatMap(lambda x: x).map(lambda x: x.upper())
        data = data.filter(lambda x: (x == "TRUMP" or x == "FLU" or x == "ZIKA" or x == "DIARRHEA" or x == "EBOLA" or x == "HEADACHE" or x == "MEASLES") )
        if data.count()>0:
            df = spark.createDataFrame(data.map(lambda x: Row(keyword = x, timestamp = time)))
            df.createOrReplaceTempView("ocurrences")
            df = spark.sql("select keyword, count(*) as total, timestamp from ocurrences group by keyword, timestamp")
            df.show()
            df.write.mode("append").saveAsTable("occurrenceTable")
        else:
            print("rdd is empty: no action")

if __name__ == "__main__":
    sc = SparkContext(appName="BDproject2 Data Frame Creation")
    readtweets()

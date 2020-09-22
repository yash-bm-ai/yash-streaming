import json

from kazoo.client import KazooClient
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


# sc = SparkContext("spark://10.0.7.120:7077", "test-kaf")
sc = SparkContext("local[8]", "test-kaf")
ssc = StreamingContext(sc, 10)
# Create a local StreamingContext with two working thread and batch interval of 1 second

offsetRanges = []


def storeOffsetRanges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd


ZOOKEEPER_SERVERS = "127.0.0.1:2181"
zc_client = None


def get_zookeeper_instance():
    global zc_client

    zc_client = KazooClient(ZOOKEEPER_SERVERS)
    zc_client.start()
    return zc_client


def read_offsets(zk, topics):
    from pyspark.streaming.kafka import TopicAndPartition

    from_offsets = {}
    try:
        for topic in topics:
            for partition in zk.get_children(f'/consumers/{topic}'):
                topic_partion = TopicAndPartition(topic, int(partition))
                offset = int(zk.get(f'/consumers/{topic}/{partition}')[0])
                from_offsets[topic_partion] = offset

    except Exception as e:
        print("Excep :: " + str(e))

    return from_offsets


def save_offsets(offsetRanges):
    zk = get_zookeeper_instance()

    print("saving-------" + str(offsetRanges))
    for offset in offsetRanges:
        print("saving -- " + str(offset.untilOffset) + " " + str(offset.partition))
        path = f"/consumers/{offset.topic}/{offset.partition}"
        zk.ensure_path(path)
        zk.set(path, str(offset.untilOffset).encode())


def handler(message):
    offsetRanges = message.offsetRanges()
    print(offsetRanges)
    records = message.collect()
    try:
        for record in records:

            j = json.loads(record[1])
            if j["-"] == 10:
                ## can retry it here
                raise Exception("Something bad happened")
            print("Printing: " + str(record))

        save_offsets(offsetRanges)
    except Exception as e:
        print("excep:" + str(e))


zk = get_zookeeper_instance()
from_offsets = read_offsets(zk, ["topic3"])

print("Offsets::::::::::::::::::::::::::::::::::")
print(from_offsets)

directKafkaStream = KafkaUtils.createDirectStream(ssc,
                                                  ["topic3"],
                                            {"metadata.broker.list": "localhost:9092,localhost:9093,localhost:9094"},
                                            fromOffsets=from_offsets)

directKafkaStream \
    .foreachRDD(handler)  # Try retry and failure cases
# .transform(storeOffsetRanges) \


ssc.start()
try:
    ssc.awaitTermination()
except Exception as e:
    print("excepppppp ::" + str(e))
    ssc.stop(stopSparkContext=True, stopGraceFully=False)

# from pyspark.sql import SparkSession
# from pyspark import SparkContext
# from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils
#
# ## TESTING
# #spark = SparkSession.builder.appName("yash-test3").getOrCreate()
# #sc = spark.sparkContext
# sc = SparkContext("local[2]", "NetworkWordCount")
# rdd = sc.parallelize([1,2,3,4,5,6,7])
# print("CYAYYYYYYYYYYY COUNTTT :::")
# print(rdd.count())
#
# # ssc = spark \
# #   .readStream \
# #   .format("kafka") \
# #   .option("kafka.bootstrap.servers", "localhost:9094, kafka://localhost:9093, kafka://localhost:9092") \
# #   .option("subscribe", "topic1") \
# #   .load()
# #df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
#
# #directKafkaStream = KafkaUtils.createDirectStream(ssc, ["topic1"], {"metadata.broker.list": brokers})
#
# ##---------------------------------------------
# # spark = SparkSession.builder.\
# #       appName("yash-test2").\
# #       getOrCreate()
# #       #master("spark://10.0.7.120:7077").\
# #
# #
# # sc = spark.sparkContext
# #
# # def handler(message):
# #     records = message.collect()
# #     for record in records:
# #          print(record)
# #
# # ssc = StreamingContext(sc, 1)
# # topic = "topic1"
# # brokers = "localhost:9092"
# # kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
# # kvs.foreachRDD(handler)
# #
# # kvs.awaitAnyTermination()
#
#
#
#

# from pyspark import SparkContext
# sc = SparkContext("local", "count app")
# words = sc.parallelize (
#    ["scala",
#    "java",
#    "hadoop",
#    "spark",
#    "akka",
#    "spark vs hadoop",
#    "pyspark",
#    "pyspark and spark"]
# )
# counts = words.count()
# print("Number of elements in RDD -> %i" % (counts))

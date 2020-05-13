from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():

    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 10)  
    ssc.checkpoint("checkpoint")
    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    positiveCounts = []
    negativeCounts = []
    for count in counts:
        if count == []:
           continue
        else:
            positiveCounts.append(count[0][1])
            negativeCounts.append(count[1][1])
    plt.plot(positiveCounts, "b",label = 'Positive')
    plt.plot(negativeCounts, "g",label = 'Negative')
    plt.ylabel("Word count")
    plt.xlabel("Time step")
    plt.show()
    plt.savefig("plot.png")


def load_wordlist(filename):
    words = []
    with open(filename, "r") as f:
        words = f.read()
        words = words.lower()
        words = words.split()
    return words



def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])
    

    
    words = tweets.flatMap(lambda tweet: tweet.split(" "))
    pairs = words.map(lambda x: ("positive", 1) if x in pwords else ("positive", 0)).union(words.map(lambda x: ("negative", 1) if x in nwords else ("negative", 0)))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)
    def updateFunc(newValues, runningCount):
        return sum(newValues, (runningCount or 0))
    runningwordCounts = pairs.updateStateByKey(updateFunc)
    runningwordCounts.pprint()
    
    counts = []
    wordCounts.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                     
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()

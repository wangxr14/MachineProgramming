from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(-1)
    sc = SparkContext(appName="WordCount")
    ssc = StreamingContext(sc, 10)
    file = sys.argv[2]
    lines = ssc.textFileStream(file)
    cnts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
    cnts.pprint()
    cnts.count().pprint()
    cnts.saveAsTextFiles("wc_result") # save result file
    cnts.count().pprint()
    ssc.start()
    ssc.awaitTermination()
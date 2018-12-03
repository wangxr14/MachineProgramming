from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    sc = SparkContext(appName="WordCount")
    ssc = StreamingContext(sc, 10)
    file = sys.argv[1]
    lines = ssc.textFileStream(file)
    cnts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
    cnts.pprint()
    cnts.saveAsTextFiles("wc_result")
    cnts.count().pprint()
    ssc.start()
    ssc.awaitTermination()

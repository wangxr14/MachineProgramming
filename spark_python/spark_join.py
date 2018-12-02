from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(-1)
    sc = SparkContext(appName="filterApp")
    ssc = StreamingContext(sc, 10)
    joinsc = sc.textFile(sc,sys.argv[2])  # local file
    lines = ssc.textFileStream(sys.argv[1]) #streaming file dir
    result = lines.join(joinsc)
    result.pprint()
    result.saveAsTextFiles("join_result")
    ssc.start()
    ssc.awaitTermination()
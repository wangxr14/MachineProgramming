from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(-1)

    sc = SparkContext(appName="filterApp")
    ssc = StreamingContext(sc, 10)
    #text = sc,sys.argv[2]  # local file
    file = sys.argv[2]
    local = open(file,'r')
    local_lines = []
    for line in local.readlines():
        local.append(line)
    lines1 = ssc.textFileStream(sys.argv[1]) #streaming file dir1
    #lines2 = ssc.textFileStream(sys.argv[2]) #streaming file dir2
    result = lines1.flatMap(lambda line: [(line + l) for l in local_lines])
    result.pprint()
    result.saveAsTextFiles("join_result")
    ssc.start()
    ssc.awaitTermination()
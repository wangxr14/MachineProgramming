from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(-1)
    sc = SparkContext(appName="FilterApp")
    ssc = StreamingContext(sc, 10)
    s = sys.argv[2]
    lines = ssc.textFileStream(sys.argv[1]) #filename
    result = lines.filter(lambda l: len(l)>0 and ( s  in l) )
    result.pprint()
    result.saveAsTextFiles("filter_result")
    ssc.start()
    ssc.awaitTermination()

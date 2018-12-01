package team.cs425.g54;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public final class JavaNetworkWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {


//        StreamingExamples.setStreamingLogLevels();

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("spark://172.22.156.180");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(20));
        JavaDStream<String> lines = ssc.textFileStream("/usr/local/spark/corpus");
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();
        wordCounts.foreachRDD(rdd ->{
            if(!rdd.isEmpty()){
                rdd.coalesce(1).saveAsTextFile("/usr/local/spark/wordcountResult");
            }
        });
        ssc.start();
        ssc.awaitTermination();
    }
}
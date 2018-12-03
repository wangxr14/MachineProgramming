package team.cs425.g54;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SparkStreaming implements Serializable {
//    SparkConf sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]");
    SparkConf sparkConf2 = new SparkConf().setAppName("SparkStreaming").setMaster("local");
//    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//    JavaSparkContext ctx2 = new JavaSparkContext(sparkConf2);

    private static FlatMapFunction<String, String> f = new FlatMapFunction<String, String>(){
        @Override
        public Iterator<String> call(String s) throws Exception {
            List<String> tmp = Arrays.asList(s.split(" "));
            ArrayList<String> names = new ArrayList<String>(tmp);
            return names.iterator();
        }
    };

    private static PairFunction<String, String, Integer> fp = new PairFunction<String, String, Integer>(){

        @Override
        public Tuple2<String, Integer> call(String s) throws Exception {
//            System.out.println(s);
            return new Tuple2(s,1);
        }
    };

    private static Function2<Integer, Integer, Integer> f2 = new Function2<Integer, Integer, Integer>(){

        @Override
        public Integer call(Integer v1, Integer v2) throws Exception {
            return v1+v2;
        }
    };
//    void test1(){
//        // input data
//        JavaRDD<String> lines = ctx.textFile("files/test.txt", 1);
//        // split up words
//
//        JavaRDD<String> words = lines.flatMap(f);
//        // Transform into word count
//        JavaPairRDD<String, Integer> counts = words.mapToPair(fp).reduceByKey(f2);
//        // save result
//
//        counts.saveAsTextFile("output_test");
//    }
    void test2() throws InterruptedException {
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf2,Durations.seconds(5));
//        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("local", 9999);
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>(){
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> tmp = Arrays.asList(s.split(" "));
                ArrayList<String> names = new ArrayList<String>(tmp);
                return names.iterator();
            }
        });
        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();

//        wordCounts.dstream().saveAsTextFiles("output2","");
//        wordCounts.dstream().print();
        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }
    public static void main(String[] args){
        SparkStreaming ss = new SparkStreaming();
        try {
            ss.test2();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }




}

package lms.learning;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class WordCount {

  public static void main(String[] args) {
    String logFile = "hdfs://localhost:9000/user/test/input/README.txt";

    SparkConf conf = new SparkConf().setAppName("Spark Word Count").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> lines = sc.textFile(logFile);

    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
    JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));
    JavaPairRDD<String, Integer> counts = ones.reduceByKey(Integer::sum);

    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<?, ?> tuple : output) {
      System.err.println("--- Word Count --- " + tuple._1() + ": " + tuple._2());
    }

    sc.stop();
  }
}

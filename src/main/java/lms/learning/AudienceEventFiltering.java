package lms.learning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;


public class AudienceEventFiltering {
  private static final String FILE_NAME = "audience-event-test.avro";
  private static final String PROFILE_FILE_NAME = "audience.avro";
  private static final String OUTPUT_FILE_NAME = "audience-event-output3";

  public static void main(String[] args) {
    String logFile = "hdfs://localhost:9000/user/test/input/" + FILE_NAME;

    SparkConf conf = new SparkConf().setAppName("Audience Event Filtering").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    Dataset<Row> events = sqlContext.read().format("avro").load(logFile);
    Dataset<Row> agg = events.groupBy(events.col("userId")).agg(functions.sum(events.col("spent")));
    agg.show();

    String outputPath = "hdfs://localhost:9000/user/test/input/" + OUTPUT_FILE_NAME + "_" + System.nanoTime();
    agg.write().format("csv").save(outputPath);

    Dataset<Row> aggregatedData = sqlContext.read().format("csv").load(outputPath + "/*.csv");
    Dataset<Row> filter = aggregatedData.filter("_c1 >= 10000.0");
    long count = filter.count();
    System.out.println("Count of filtered user ids " + count);
    filter.show();

    Dataset<Row> profiles =
        sqlContext.read().format("avro").load("hdfs://localhost:9000/user/test/input/" + PROFILE_FILE_NAME);
    Dataset<Row> joinedDataSet = filter.join(profiles, profiles.col("id").equalTo(aggregatedData.col("_c0")));
    joinedDataSet.select("id", "email", "name", "_c1").show();

    sc.stop();
  }
}

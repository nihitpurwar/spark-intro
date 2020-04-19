package lms.learning;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class AudienceEventAvroWriter {

  private static final int TOTAL_AUDIENCE = 10000;
  private static final String FILE_NAME = "audience-event-test.avro";

  public static void main(String[] args) {
    // First thing - parse the schema as it will be used
    Schema schema = parseSchema();
    createAndWriteAudienceProfiles(schema);
  }

  private static Schema parseSchema() {
    Schema.Parser parser = new Schema.Parser();
    Schema schema;
    try {
      // pass path to schema
      InputStream systemResourceAsStream = ClassLoader.getSystemResourceAsStream("AudienceEventSchema.avsc");
      schema = parser.parse(systemResourceAsStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return schema;
  }

  private static void createAndWriteAudienceProfiles(Schema schema) {
    DatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericData.Record> dataFileWriter = null;
    try {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/user/test/input/" + FILE_NAME), conf);
      OutputStream out = fs.create(new Path("hdfs://localhost:9000/user/test/input/" + FILE_NAME));

      dataFileWriter = new DataFileWriter<>(datumWriter);
      // for compression
      // dataFileWriter.setCodec(CodecFactory.snappyCodec());
      dataFileWriter.create(schema, out);

      Random random = new Random();
      random.setSeed(System.currentTimeMillis());

      for (int profileId = 0; profileId < TOTAL_AUDIENCE; profileId++) {
        // total 1 million events
        for (int eventNum = 0; eventNum < 100; eventNum++) {

          GenericData.Record event = new GenericData.Record(schema);
          event.put("id", profileId + "_" + eventNum);
          event.put("transactionId", profileId + "_" + eventNum);
          event.put("productId", String.valueOf(random.nextInt(10)));
          event.put("userId", profileId);

          // profiles 0-999 will spend $1000, profiles 1000-1999 will spend total $2000
          int totalSpent = (profileId / 1000 + 1) * 1000;
          //noinspection IntegerDivisionInFloatingPointContext
          double spendPerEvent = totalSpent / 100;

          event.put("spent", spendPerEvent);
          dataFileWriter.append(event);
        }

        dataFileWriter.flush();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (dataFileWriter != null) {
        try {
          dataFileWriter.close();
        } catch (IOException e) {
          System.err.println("Exception occurred " + e.getMessage());
        }
      }
    }
  }
}
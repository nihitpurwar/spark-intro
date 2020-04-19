package lms.learning;

import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


public class AudienceEventAvroReader {
//  private static final String FILE_NAME = "audience-event-test.avro";
  private static final String FILE_NAME = "audience-event-output1.avro";

  public static void main(String[] args) {
    // First thing - parse the schema as it will be used
    Schema schema = parseSchema();
    readFromAvroFile(schema);
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

  private static void readFromAvroFile(Schema schema) {

    Configuration conf = new Configuration();
    DataFileReader<GenericData.Record> dataFileReader = null;
    try {
      FsInput in = new FsInput(new Path("hdfs://localhost:9000/user/test/input/" + FILE_NAME), conf);
      DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>(schema);
      dataFileReader = new DataFileReader<>(in, datumReader);
      GenericData.Record profile = null;
      while (dataFileReader.hasNext()) {
        profile = dataFileReader.next(profile);
        System.out.println(profile);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (dataFileReader != null) {
        try {
          dataFileReader.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
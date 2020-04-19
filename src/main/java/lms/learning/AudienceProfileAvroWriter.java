package lms.learning;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class AudienceProfileAvroWriter {

  private static final int TOTAL_AUDIENCE = 10000;
  private static final String FILE_NAME = "audience.avro";

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
      InputStream systemResourceAsStream = ClassLoader.getSystemResourceAsStream("AudienceSchema.avsc");
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

      for (int iterationNum = 0; iterationNum < TOTAL_AUDIENCE; iterationNum++) {
        GenericData.Record profile = new GenericData.Record(schema);
        profile.put("id", iterationNum);
        profile.put("name", "Jack" + iterationNum);
        profile.put("email", iterationNum + "jack@gmail.com");

        dataFileWriter.append(profile);

        if (iterationNum % 1000 == 0) {
          dataFileWriter.flush();
        }
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
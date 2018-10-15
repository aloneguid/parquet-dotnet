import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {

   private static final Configuration conf = new Configuration();

   public static void main(String[] args) throws IOException {

      Path file = new Path("C:\\dev\\parquet-dotnet\\src\\Parquet.Test\\data\\customer.impala.parquet");
      Path outUncompressed = new Path("c:\\tmp\\java.uncompressed.parquet");
      Path outGzipped = new Path("c:\\tmp\\java.gzip.parquet");

      List<Long> readTimes = new ArrayList<Long>();
      List<Long> writeUTimes = new ArrayList<Long>();
      List<Long> writeGTimes = new ArrayList<Long>();
      List<GenericRecord> allRecords = new ArrayList<GenericRecord>();
      Schema schema = null;

      for(int i = 0; i < 11; i++) {

         //read
         TimeWatch readTime = TimeWatch.start();
         ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file).build();
         GenericRecord record;
         while((record = reader.read()) != null) {
            if(i == 0) {
               //add once
               allRecords.add(record);
               if(schema == null) {
                  schema = record.getSchema();
               }
            }
         }
         reader.close();
         long readMs = readTime.time();
         if(i != 0) {
            readTimes.add(readMs);
         }

         //write (uncompressed)
         File t = new File(outUncompressed.toString());
         t.delete();
         TimeWatch writeUnc = TimeWatch.start();
         ParquetWriter<GenericRecord> writer = AvroParquetWriter
            .<GenericRecord>builder(outUncompressed)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withSchema(schema)
            .build();
         for(GenericRecord wr: allRecords) {
            writer.write(wr);
         }
         writer.close();
         if(i != 0) {
            writeUTimes.add((writeUnc.time()));
         }

         writeTest(i, CompressionCodecName.UNCOMPRESSED, writeUTimes, outUncompressed,
            schema, allRecords);

         writeTest(i, CompressionCodecName.GZIP, writeGTimes, outGzipped,
            schema, allRecords);
      }

      System.out.println("mean (read): " + avg(readTimes));
      System.out.println("mean (write uncompressed): " + avg(writeUTimes));
      System.out.println("mean (write gzip): " + avg(writeGTimes));
   }

   private static void writeTest(int iteration, CompressionCodecName codec, List<Long> times,
                                 Path destPath, Schema schema, List<GenericRecord> records) throws IOException {
      File t = new File(destPath.toString());
      t.delete();
      TimeWatch timer = TimeWatch.start();
      ParquetWriter<GenericRecord> writer = AvroParquetWriter
         .<GenericRecord>builder(destPath)
         .withCompressionCodec(codec)
         .withSchema(schema)
         .build();
      for(GenericRecord wr: records) {
         writer.write(wr);
      }
      writer.close();
      if(iteration != 0) {
         times.add(timer.time());
      }
   }

   private static Long avg(List<Long> list) {
      long sum = 0;
      for(Long time : list) {
         sum += time;
      }

      return sum / list.size();
   }
}

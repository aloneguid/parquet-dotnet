using System;
using Parquet;
using Parquet.Data;
using LogMagic;
using NetBox;
using System.Collections.Generic;
using System.Linq;
using F = System.IO.File;
using System.Runtime;

namespace Parquet.Runner
{
   class Program
   {
      private static readonly LogMagic.ILog log = LogMagic.L.G(typeof(Program));

      static void Main(string[] args)
      {
         L.Config
            .WriteTo.PoshConsole();

         for (int i = 0; i < 10; i++)
         {
            Debug();
         }
         //Perf();
      }


      private static void Debug()
      {
         //GCSettings.LatencyMode = GCLatencyMode.LowLatency;
         DataSet ds = ParquetReader.ReadFile(@"C:\dev\parquet-dotnet\src\Parquet.Test\data\customer.impala.parquet", new ParquetOptions { TreatByteArrayAsString = true });
      }

      private static void Perf()
      {
         var readTimes = new List<TimeSpan>();
         var writeUncompressedTimes = new List<TimeSpan>();
         var writeGzipTimes = new List<TimeSpan>();
         var writeSnappyTimes = new List<TimeSpan>();

         for (int i = 0; i < 4; i++)
         {
            DataSet ds;

            using (var time = new TimeMeasure())
            {
               ds = ParquetReader.ReadFile("C:\\tmp\\customer.impala.parquet");
               TimeSpan elapsed = time.Elapsed;
               readTimes.Add(elapsed);
               log.Trace("read in {0}", elapsed);
            }

            /*string dest = "c:\\tmp\\write.test.parquet";
            if (F.Exists(dest)) F.Delete(dest);

            using (var time = new TimeMeasure())
            {
               ParquetWriter.WriteFile(ds, dest, CompressionMethod.None);
               writeUncompressedTimes.Add(time.Elapsed);
            }

            using (var time = new TimeMeasure())
            {
               ParquetWriter.WriteFile(ds, dest, CompressionMethod.Gzip);
               writeGzipTimes.Add(time.Elapsed);
            }

            using (var time = new TimeMeasure())
            {
               ParquetWriter.WriteFile(ds, dest, CompressionMethod.Snappy);
               writeSnappyTimes.Add(time.Elapsed);
            }*/

            log.Trace("run finished: {0}", i);
         }

         double avgRead = readTimes.Skip(1).Average(t => t.TotalMilliseconds);
         log.Trace("avg: {0}", avgRead);

         /*double avgUncompressed = writeUncompressedTimes.Skip(1).Average(t => t.TotalMilliseconds);
         double avgGzip = writeGzipTimes.Skip(1).Average(t => t.TotalMilliseconds);
         double avgSnappy = writeUncompressedTimes.Skip(1).Average(t => t.TotalMilliseconds);

         log.Trace("averages => read: {0}, uncompressed: {1}, gzip: {2}, snappy: {3}", avgRead, avgUncompressed, avgGzip, avgSnappy);*/

      }
   }
}
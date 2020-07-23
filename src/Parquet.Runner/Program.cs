using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NetBox.Performance;
using Parquet.Data;
using F = System.IO.File;

namespace Parquet.Runner
{
   class Program
   {
      static void Main(string[] args)
      {
         //CompressOld("c:\\tmp\\DSL.svg", "c:\\tmp\\DSL.svg.oldsnappy");
         //CompressNew("c:\\tmp\\DSL.svg", "c:\\tmp\\DSL.svg.newsnappy");

         //CompressOld("c:\\tmp\\rfc8660long.txt", "c:\\tmp\\rfc8660long.txt.oldsnappy");
         CompressNew("c:\\tmp\\rfc8660long.txt", "c:\\tmp\\rfc8660long.txt.newsnappy");
      }

      static void CompressNew(string src, string dest)
      {
         using (FileStream streamDest = F.OpenWrite(dest))
         {
            using (Stream streamSnappy = IronSnappy.Snappy.OpenWriter(streamDest))
            {
               using(FileStream streamSrc = F.OpenRead(src))
               {
                  using(var time = new TimeMeasure())
                  {
                     streamSrc.CopyTo(streamSnappy);

                     TimeSpan duration = time.Elapsed;

                     Console.WriteLine($"new: {src} => {dest}. {duration} {new FileInfo(dest).Length}");
                  }
               }
            }
         }
      }

      private static async Task PerfAsync()
      {
         var readTimes = new List<TimeSpan>();
         var uwts = new List<TimeSpan>();
         var gwts = new List<TimeSpan>();
         for (int i = 0; i < 10; i++)
         {
            (TimeSpan readTime, TimeSpan uwt, TimeSpan gwt) = await ReadLargeFileAsync().ConfigureAwait(false);
            readTimes.Add(readTime);
            uwts.Add(uwt);
            gwts.Add(gwt);
            Console.WriteLine("iteration #{0}: {1}, uwp: {2}, gwt: {3}", i, readTime, uwt, gwt);
         }

         Console.WriteLine("mean(read): {0}, mean(uw): {1}, mean(gw): {2}",
            TimeSpan.FromTicks((long)readTimes.Average(t => t.Ticks)),
            TimeSpan.FromTicks((long)uwts.Average(t => t.Ticks)),
            TimeSpan.FromTicks((long)gwts.Average(t => t.Ticks)));

      }

      private static async Task<(TimeSpan ReadTime, TimeSpan UncompressedWriteTime, TimeSpan GzipWriteTime)>ReadLargeFileAsync() //TODO changed the signature because async methods cannot have out params, need to validate
      {
         TimeSpan readTime, uncompressedWriteTime, gzipWriteTime;

         Schema schema;
         DataColumn[] columns;

         using (var time = new TimeMeasure())
         {
            await using (var reader = ParquetReader.OpenFromFile(@"C:\dev\parquet-dotnet\src\Parquet.Test\data\customer.impala.parquet", new ParquetOptions { TreatByteArrayAsString = true }))
            {
               schema = reader.Schema;
               var cl = new List<DataColumn>();

               using (ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0))
               {
                  foreach (DataField field in reader.Schema.GetDataFields())
                  {
                     DataColumn dataColumn = await rgr.ReadColumnAsync(field).ConfigureAwait(false);
                     cl.Add(dataColumn);
                  }
               }
               columns = cl.ToArray();
            }
            readTime = time.Elapsed;
         }

         using (FileStream dest = F.OpenWrite("perf.uncompressed.parquet"))
         {
            using (var time = new TimeMeasure())
            {
               await using (var writer = new ParquetWriter(schema, dest))
               {
                  writer.CompressionMethod = CompressionMethod.None;
                  using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
                  {
                     foreach (DataColumn dc in columns)
                     {
                        await rg.WriteColumnAsync(dc).ConfigureAwait(false);
                     }
                  }
               }

               uncompressedWriteTime = time.Elapsed;
            }
         }


         using (FileStream dest = F.OpenWrite("perf.gzip.parquet"))
         {
            using (var time = new TimeMeasure())
            {
               await using (var writer = new ParquetWriter(schema, dest))
               {
                  writer.CompressionMethod = CompressionMethod.Gzip;
                  using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
                  {
                     foreach (DataColumn dc in columns)
                     {
                        await rg.WriteColumnAsync(dc).ConfigureAwait(false);
                     }
                  }
               }

               gzipWriteTime = time.Elapsed;
            }
         }

         return (readTime, uncompressedWriteTime, gzipWriteTime);
      }
   }
}
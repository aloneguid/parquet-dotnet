using System;
using System.Collections.Generic;
using System.Linq;
using LogMagic;
using Parquet.Data;
using Parquet.File;

namespace Parquet.Runner
{
   class Program
   {
      private static readonly LogMagic.ILog log = LogMagic.L.G(typeof(Program));

      static void Main(string[] args)
      {
         L.Config
            .WriteTo.PoshConsole();

         var times = new List<TimeSpan>();
         for (int i = 0; i < 10; i++)
         {
            using (var time = new TimeMeasure())
            {
               ReadLargeFile();
               times.Add(time.Elapsed);
               log.Trace("iteration #{0}: {1}", i, time.Elapsed);
            }
         }

         log.Trace("mean: {0}", TimeSpan.FromTicks((long)times.Average(t => t.Ticks)));
         Console.ReadKey();
      }


      private static void ReadLargeFile()
      {
         using (var reader = ParquetReader.OpenFromFile(@"C:\dev\parquet-dotnet\src\Parquet.Test\data\customer.impala.parquet", new ParquetOptions { TreatByteArrayAsString = true }))
         {
            using (ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0))
            {
               foreach(DataField field in reader.Schema.GetDataFields())
               {
                  DataColumn dataColumn = rgr.ReadColumn(field);
               }
            }
         }
      }
   }
}
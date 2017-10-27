using System;
using Parquet;
using Parquet.Data;
using LogMagic;
using NetBox;
using System.Collections.Generic;
using System.Linq;

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
               ParquetReader.ReadFile("C:\\dev\\parquet-dotnet\\src\\Parquet.Test\\data\\customer.impala.parquet");
               times.Add(time.Elapsed);

               log.Trace("{0}", time.Elapsed);
            }
         }

         double average = times.Skip(1).Average(t => t.TotalMilliseconds);

         log.Trace("average: {0}", average);

      }
   }
}
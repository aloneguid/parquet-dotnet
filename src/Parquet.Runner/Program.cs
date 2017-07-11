using System;
using Parquet;
using Parquet.Data;
using LogMagic;

namespace Parquet.Runner
{
   class Program
   {
      static void Main(string[] args)
      {
         L.Config
            .WriteTo.PoshConsole();

         DataSet ds;
         using (var time = new TimeMeasure())
         {
            ds = ParquetReader.ReadFile("C:\\tmp\\postcodes.plain.parquet");

            Console.WriteLine("read in {0}", time.Elapsed);
         }

         Console.WriteLine("has {0} rows", ds.RowCount);

         //postcodes.plain.parquet - 137Mb
         //debug: 26 seconds.
         //release: 25 seconds.
      }
   }
}
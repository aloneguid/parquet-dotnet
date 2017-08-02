using System;
using Parquet;
using Parquet.Data;
using LogMagic;
using NetBox;

namespace Parquet.Runner
{
   class Program
   {
      private static readonly LogMagic.ILog log = LogMagic.L.G(typeof(Program));

      static void Main(string[] args)
      {
         L.Config
            .WriteTo.PoshConsole();

         using (var time = new TimeMeasure())
         {
            var ds = new DataSet(
               new SchemaElement<int>("id"),
               new SchemaElement<string>("name"),
               new SchemaElement<double>("lat"),
               new SchemaElement<double>("lon"));

            log.D(ds.Schema.Show());

            for (int i = 0; i < 10; i++)
            {
               ds.Add(
                  i,
                  NameGenerator.GeneratePersonFullName(),
                  Generator.RandomDouble,
                  Generator.RandomDouble);
            }

            ParquetWriter.WriteFile(ds, "c:\\tmp\\perf.parquet");


            log.D("written in {0}", time.Elapsed);
         }

      }
   }
}
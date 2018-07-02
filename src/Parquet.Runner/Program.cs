using System;
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

         ReadLargeFile();

         log.Trace("done!");
         Console.ReadKey();
      }


      private static void ReadLargeFile()
      {
         using (var reader = ParquetReader.OpenFromFile(@"C:\dev\parquet-dotnet\src\Parquet.Test\data\customer.impala.parquet", new ParquetOptions { TreatByteArrayAsString = true }))
         {
            log.Trace("row groups: {0}", reader.RowGroupCount);

            using (ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0))
            {
               foreach(DataField field in reader.Schema.GetDataFields())
               {
                  DataColumn dataColumn = rgr.ReadColumn(field);

                  log.Trace("col {0}, values: {1}", field, dataColumn.Data.Length);
               }
            }
         }
      }
   }
}
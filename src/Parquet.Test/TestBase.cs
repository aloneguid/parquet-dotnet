using System;
using System.IO;
using Parquet.Data;
using System.Linq;
using F = System.IO.File;
using Parquet.Data.Rows;

namespace Parquet.Test
{
   public class TestBase
   {
      protected Stream OpenTestFile(string name)
      {
         return F.OpenRead("./data/" + name);
      }

      protected Table ReadTestFileAsTable(string name)
      {
         using (Stream s = OpenTestFile(name))
         {
            using (var reader = new ParquetReader(s))
            {
               return reader.ReadAsTable();
            }
         }
      }

      protected Table WriteRead(Table table, bool saveLocal = false)
      {
         var ms = new MemoryStream();

         using (var writer = new ParquetWriter(table.Schema, ms))
         {
            writer.Write(table);
         }

         if(saveLocal)
         {
            F.WriteAllBytes("c:\\tmp\\test.parquet", ms.ToArray());
         }

         ms.Position = 0;

         using (var reader = new ParquetReader(ms))
         {
            return reader.ReadAsTable();
         }
      }

      protected DataColumn WriteReadSingleColumn(DataField field, DataColumn dataColumn)
      {
         using (var ms = new MemoryStream())
         {
            // write with built-in extension method
            ms.WriteSingleRowGroupParquetFile(new Schema(field), dataColumn);
            ms.Position = 0;

            // read first gow group and first column
            using (var reader = new ParquetReader(ms))
            {
               if (reader.RowGroupCount == 0) return null;
               ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0);

               return rgReader.ReadColumn(field);
            }


         }
      }

      protected DataColumn[] WriteReadSingleRowGroup(Schema schema, DataColumn[] columns, out Schema readSchema)
      {
         using (var ms = new MemoryStream())
         {
            ms.WriteSingleRowGroupParquetFile(schema, columns);
            ms.Position = 0;

            using (var reader = new ParquetReader(ms))
            {
               readSchema = reader.Schema;

               using (ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0))
               {
                  return columns.Select(c =>
                     rgReader.ReadColumn(c.Field))
                     .ToArray();

               }
            }
         }
      }

      protected object WriteReadSingle(DataField field, object value, CompressionMethod compressionMethod = CompressionMethod.None)
      {
         //for sanity, use disconnected streams
         byte[] data;

         using (var ms = new MemoryStream())
         {
            // write single value

            using (var writer = new ParquetWriter(new Schema(field), ms))
            {
               writer.CompressionMethod = compressionMethod;

               using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
               {
                  Array dataArray = Array.CreateInstance(field.ClrNullableIfHasNullsType, 1);
                  dataArray.SetValue(value, 0);
                  var column = new DataColumn(field, dataArray);

                  rg.WriteColumn(column);
               }
            }

            data = ms.ToArray();
         }

         using (var ms = new MemoryStream(data))
         { 
            // read back single value

            ms.Position = 0;
            using (var reader = new ParquetReader(ms))
            {
               using (ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0))
               {
                  DataColumn column = rowGroupReader.ReadColumn(field);

                  return column.Data.GetValue(0);
               }
            }
         }
      }
   }
}
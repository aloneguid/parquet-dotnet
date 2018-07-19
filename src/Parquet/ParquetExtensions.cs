using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;
using Parquet.File;

namespace Parquet
{
   /// <summary>
   /// Defines extension methods to simplify Parquet usage (experimental v3)
   /// </summary>
   public static class ParquetExtensions
   {
      /// <summary>
      /// Writes a file with a single row group
      /// </summary>
      public static void WriteSingleRowGroupParquetFile(this Stream stream, Schema schema, int rowCount, params DataColumn[] columns)
      {
         using (var writer = new ParquetWriter(schema, stream))
         {
            writer.CompressionMethod = CompressionMethod.None;
            using (ParquetRowGroupWriter rgw = writer.CreateRowGroup(rowCount))
            {
               foreach(DataColumn column in columns)
               {
                  rgw.Write(column);
               }
            }
         }
      }

      /// <summary>
      /// Reads the first row group from a file
      /// </summary>
      /// <param name="stream"></param>
      /// <param name="schema"></param>
      /// <param name="columns"></param>
      public static void ReadSingleRowGroupFile(this Stream stream, out Schema schema, out DataColumn[] columns)
      {
         using (var reader = new ParquetReader(stream))
         {
            schema = reader.Schema;

            using (ParquetRowGroupReader rgr = reader.OpenRowGroupReader(0))
            {
               DataField[] dataFields = schema.GetDataFields();
               columns = new DataColumn[dataFields.Length];

               for(int i = 0; i < dataFields.Length; i++)
               {
                  columns[i] = rgr.ReadColumn(dataFields[i]);
               }
            }
         }
      }
   }
}
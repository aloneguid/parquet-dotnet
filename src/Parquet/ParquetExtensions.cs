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
   internal static class ParquetExtensions
   {
      public static void WriteSingleRowGroup(this Stream stream, Schema schema, int rowCount, params DataColumn[] columns)
      {
         using (var writer = new ParquetWriter3(schema, stream))
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
   }
}
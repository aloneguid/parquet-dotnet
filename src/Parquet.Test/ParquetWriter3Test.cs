using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;
using Parquet.File;
using Xunit;

namespace Parquet.Test
{
   public class ParquetWriter3Test
   {
      [Fact]
      public void Write_simplest_int_and_string_columns_in_one_row_group()
      {
         var schema = new Schema(new DataField<int>("id"), new DataField<string>("name"));

         using (var ms = new MemoryStream())
         {
            using (var writer = new ParquetWriter3(schema, ms))
            {
               writer.CompressionMethod = CompressionMethod.None;

               using (ParquetRowGroupWriter group = writer.CreateRowGroup(3))
               {
                  group.Write(CreateColumn(schema[0], 1, 2, 3));
                  group.Write(CreateColumn(schema[1], "first", "second", "third"));
               }
            }
         }
      }

      private DataColumn CreateColumn<T>(Field f, params T[] values)
      {
         var df = (DataField)f;
         var list = new List<T>(values);

         return new DataColumn(df, list);
      }
   }
}

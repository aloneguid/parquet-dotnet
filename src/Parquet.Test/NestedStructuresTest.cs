using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Xunit;

namespace Parquet.Test
{
   public class NestedStructuresTest
   {
      [Fact]
      public void Simple_nested_structure_write_read()
      {
         var ds = new DataSet(
            new SchemaElement<int>("id"),
            new SchemaElement<Row>("address",
               new SchemaElement<string>("line1"),
               new SchemaElement<string>("postcode")
            ));

         ds.Add(1, new Row("flat 4", "se11"));
         Assert.Equal(1, ds.RowCount);
         Assert.Equal(2, ds.ColumnCount);

         DataSet ds1 = DataSetGenerator.WriteRead(ds);

         ParquetWriter.WriteFile(ds, "c:\\tmp\\firstnested.parquet");
      }
   }
}
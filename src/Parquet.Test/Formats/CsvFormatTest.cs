using Parquet.Data;
using Parquet.Formats;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Xunit;

namespace Parquet.Test.Formats
{
   public class CsvFormatTest
   {
      [Fact]
      public void Reads_alltypes_inferred()
      {
         DataSet ds;

         using (var csvs = System.IO.File.OpenRead(GetDataFilePath("alltypes.csv")))
         {
            ds = CsvFormat.ReadToDataSet(csvs, new CsvOptions { InferSchema = true, HasHeaders = true });
         }

         Assert.Equal(8, ds.RowCount);
         Assert.Equal(typeof(int), ds.Schema[0].ElementType);
         Assert.Equal(typeof(bool), ds.Schema[1].ElementType);
         Assert.Equal(typeof(int), ds.Schema[2].ElementType);
         Assert.Equal(typeof(int), ds.Schema[3].ElementType);
         Assert.Equal(typeof(int), ds.Schema[4].ElementType);
         Assert.Equal(typeof(int), ds.Schema[5].ElementType);
         Assert.Equal(typeof(float), ds.Schema[6].ElementType);
         Assert.Equal(typeof(float), ds.Schema[7].ElementType);
         Assert.Equal(typeof(DateTimeOffset), ds.Schema[8].ElementType);
         Assert.Equal(typeof(int), ds.Schema[9].ElementType);
         Assert.Equal(typeof(DateTimeOffset), ds.Schema[10].ElementType);
      }

      [Fact]
      public void Reads_csv_with_no_headers()
      {
         DataSet ds;

         using (var csvs = System.IO.File.OpenRead(GetDataFilePath("alltypes_no_headers.csv")))
         {
            ds = CsvFormat.ReadToDataSet(csvs, new CsvOptions { InferSchema = true, HasHeaders = false });
         }

         Assert.Equal(8, ds.RowCount);
      }

      [Fact]
      public void Reads_csv_with_heads_no_type_inferring()
      {
         DataSet ds = CsvFormat.ReadToDataSet(GetDataFilePath("alltypes.csv"), new CsvOptions { InferSchema = false, HasHeaders = true });

         Assert.Equal(8, ds.RowCount);
         Assert.True(ds.Schema.Elements.All(se => se.ElementType == typeof(string)));
      }

      [Fact]
      public void Reads_csv_without_heads_no_type_inferring()
      {
         DataSet ds = CsvFormat.ReadToDataSet(GetDataFilePath("alltypes_no_headers.csv"), new CsvOptions { InferSchema = false, HasHeaders = false });

         Assert.Equal(8, ds.RowCount);
         Assert.True(ds.Schema.Elements.All(se => se.ElementType == typeof(string)));
      }

      private string GetDataFilePath(string name)
      {
         string thisPath = typeof(CsvFormatTest).GetTypeInfo().Assembly.Location;
         return Path.Combine(Path.GetDirectoryName(thisPath), "data", name);
      }
   }
}

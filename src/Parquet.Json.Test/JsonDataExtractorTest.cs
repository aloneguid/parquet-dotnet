using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Parquet.Data;
using Xunit;

namespace Parquet.Json.Test
{
   public class JsonDataExtractorTest : TestBase
   {
      [Fact]
      public void Read_simple_json_with_no_nesting()
      {
         JObject jo = JObject.Parse(ReadJson("flat.json"));

         Schema schema = jo.InferParquetSchema();

         DataSet ds = jo.ToParquetDataSet(schema);

         //validate schema
         Schema s = ds.Schema;
         Assert.Equal(3, s.Length);
         Assert.Equal(new DataField<int?>("id"), s[0]);
         Assert.Equal(new DataField<string>("country"), s[1]);
         Assert.Equal(new DataField<int?>("population"), s[2]);

         //validate data
         Assert.Equal(1, ds.RowCount);
         Assert.Equal("{123;UK;1300000}", ds[0].ToString());
      }

      [Fact]
      public void Read_perfect_struct()
      {
         JObject jo = JObject.Parse(ReadJson("struct.json"));
         Schema schema = jo.InferParquetSchema();
         DataSet ds = jo.ToParquetDataSet(schema);

         //validate schema
         Assert.Equal(3, schema.Length);
         Assert.Equal(new DataField<int?>("id"), schema[0]);
         Assert.Equal(new DataField<string>("country"), schema[1]);
         Assert.Equal(new StructField("population", new DataField<int?>("year"), new DataField<int?>("amount")), schema[2]);

         //validate data
         Assert.Equal(1, ds.RowCount);
         Assert.Equal("{123;UK;{2017;12345}}", ds[0].ToString());
      }

      [Fact]
      public void Read_perfect_array()
      {
         JObject jo = JObject.Parse(ReadJson("array.json"));
         Schema schema = jo.InferParquetSchema();

         Assert.Equal(
            new Schema(new DataField<int?>("id"), new DataField<IEnumerable<string>>("countries")),
            schema);

         DataSet ds = jo.ToParquetDataSet(schema);
         Assert.Equal("{123;[UK;US]}", ds[0].ToString());
      }
   }
}
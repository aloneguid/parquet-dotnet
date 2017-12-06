using System.Collections.Generic;
using System.IO;
using System.Linq;
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

      [Fact]
      public void Read_perfect_list_of_structs()
      {
         JObject jo = JObject.Parse(ReadJson("listofstructs.json"));
         Schema schema = jo.InferParquetSchema();

         Assert.Equal(
            new Schema(
               new DataField<int?>("id"),
               new DataField<string>("country"),
               new ListField("population",
                  new StructField("item",
                     new DataField<int?>("year"),
                     new DataField<int?>("population")
                  )
               )
            ),
            schema);

         DataSet ds = jo.ToParquetDataSet(schema);
         Assert.Equal("{123;UK;[{2017;111};{2018;222}]}", ds[0].ToString());
      }

      [Fact]
      public void Read_multiple_with_missing_data()
      {
         var schema = new Schema(
            new DataField<int?>("id"),
            new DataField<string>("country"),
            new StructField("population",
               new DataField<int?>("year"),
               new DataField<int?>("amount"),
               new DataField<int?>("diff")),
            new DataField<string>("comment"));

         var extractor = new JsonDataExtractor(schema);
         JObject doc1 = JObject.Parse(ReadJson("infer00.json"));
         JObject doc2 = JObject.Parse(ReadJson("infer01.json"));
         var ds = new DataSet(schema);
         extractor.AddRow(ds, doc1);
         extractor.AddRow(ds, doc2);

         Assert.Equal(2, ds.RowCount);
         Assert.Equal("{123;UK;{2016;111;<null>};<null>}", ds[0].ToString());
         Assert.Equal("{123;UK;{2017;222;111};no comments}", ds[1].ToString());
      }

      //[Fact]
      public void TempTest()
      {
         var dir = new DirectoryInfo(@"C:\Users\ivang\Downloads\Fullfeed-20170330004044");
         FileInfo[] files = dir.GetFiles();
         JObject[] jos = files
            .Select(fi => JObject.Parse(System.IO.File.ReadAllText(fi.FullName)))
            .Take(1000)
            .ToArray();

         var inferrer = new JsonSchemaInferring();
         Schema schema = inferrer.InferSchema(jos);

         var extractor = new JsonDataExtractor(schema);
         var ds = new DataSet(schema);
         for(int i = 0; i < jos.Length; i++)
         {
            extractor.AddRow(ds, jos[i]);
         }

         ParquetWriter.WriteFile(ds, "c:\\tmp\\com.parquet");
      }
   }
}
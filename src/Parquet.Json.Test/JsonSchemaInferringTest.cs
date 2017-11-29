using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json.Linq;
using Parquet.Data;
using Xunit;

namespace Parquet.Json.Test
{
   public class JsonSchemaInferringTest : TestBase
   {
      [Fact]
      public void Infer_different_schemas()
      {
         var inferrer = new JsonSchemaInferring();

         JObject doc1 = JObject.Parse(ReadJson("infer00.json"));
         JObject doc2 = JObject.Parse(ReadJson("infer01.json"));

         Schema schema = inferrer.InferSchema(new[] {doc1, doc2});

         Assert.Equal(
            new Schema(
               new DataField<int?>("id"),
               new DataField<string>("country"),
               new StructField("population",
                  new DataField<int?>("year"),
                  new DataField<int?>("amount"),
                  new DataField<int?>("diff")),
               new DataField<string>("comment")),
            schema);
      }
   }
}

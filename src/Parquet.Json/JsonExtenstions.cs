using System;
using Parquet.Data;
using Parquet.Json;
using PSchema = Parquet.Data.Schema;

namespace Newtonsoft.Json.Linq
{
   public static class JsonExtenstions
   {
      public static DataSet ToParquetDataSet(this JObject jObject)
      {
         //extract schema
         var schemaExtractor = new JsonSchemaExtractor();
         schemaExtractor.Analyze(jObject);
         PSchema schema = schemaExtractor.GetSchema();

         //convert data
         var dataExtractor = new JsonDataExtractor(schema);
         var ds = new DataSet(schema);
         dataExtractor.AddRow(ds, jObject);

         return ds;
      }
   }
}

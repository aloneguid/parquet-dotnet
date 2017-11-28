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

         //convert
         //todo

         return new DataSet(schema);
      }
   }
}

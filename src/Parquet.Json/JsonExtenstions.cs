using System;
using Parquet.Data;
using Parquet.Json;

namespace Newtonsoft.Json.Linq
{
   public static class JsonExtenstions
   {
      public static DataSet ToParquetDataSet(this JObject jObject)
      {
         var schemaExtractor = new JsonSchemaExtractor();
         schemaExtractor.Analyze(jObject);

         return null;
      }
   }
}

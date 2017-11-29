using System;
using Parquet.Data;
using Parquet.Json;
using PSchema = Parquet.Data.Schema;

namespace Newtonsoft.Json.Linq
{
   public static class JsonExtenstions
   {
      public static PSchema InferParquetSchema(this JObject jObject)
      {
         var schemaExtractor = new JsonSchemaInferring();
         PSchema schema = schemaExtractor.InferSchema(new[] { jObject });
         return schema;
      }

      public static DataSet ToParquetDataSet(this JObject jObject, PSchema schema)
      {
         if (schema == null)
         {
            throw new ArgumentNullException(nameof(schema));
         }

         //convert data
         var dataExtractor = new JsonDataExtractor(schema);
         var ds = new DataSet(schema);
         dataExtractor.AddRow(ds, jObject);

         return ds;
      }

      internal static bool IsPrimitiveValue(this JToken jt)
      {
         return jt.Type >= JTokenType.Integer;
      }
   }
}

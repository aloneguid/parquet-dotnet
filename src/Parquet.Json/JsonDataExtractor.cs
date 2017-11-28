using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Parquet.Data;

namespace Parquet.Json
{
   class JsonDataExtractor
   {
      private readonly Schema _schema;

      public JsonDataExtractor(Schema schema)
      {
         _schema = schema;
      }

      public void AddRow(DataSet ds, JObject jo)
      {
         var values = new List<object>();

         foreach(Field field in _schema.Fields)
         {
            string path = GetJsonPath(field);

            JToken vt = jo.SelectToken(path);
            JValue jv = (JValue)vt;

            object value = GetValue(jv.Value, (DataField)field);
            values.Add(value);
         }

         var row = new Row(values);
         ds.Add(row);

      }

      private string GetJsonPath(Field field)
      {
         return "$." + field.Name;
      }

      private object GetValue(object jsonValue, DataField df)
      {
         switch(df.DataType)
         {
            case DataType.Int32:
               return new int?((int)(long)jsonValue);
            case DataType.String:
               return (string)jsonValue;
            default:
               throw new NotImplementedException();
         }
      }
   }
}

using System;
using System.Collections;
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

      /// <summary>
      /// Adds a row to an existing DataSet by converting it from specified JSON object using specified schema
      /// </summary>
      /// <param name="ds"></param>
      /// <param name="jo"></param>
      public void AddRow(DataSet ds, JObject jo)
      {
         ds.Add(CreateRow(jo, _schema.Fields));
      }

      private Row CreateRow(JObject jo, IEnumerable<Field> fields)
      {
         var values = new List<object>();

         foreach (Field field in fields)
         {
            string path = GetJsonPath(field);
            object value;

            switch(field.SchemaType)
            {
               case SchemaType.Data:
                  JToken vt = jo.SelectToken(path);
                  value = vt.Type == JTokenType.Array
                     ? GetValues((JArray)vt, (DataField)field)
                     : GetValue(((JValue)vt)?.Value, (DataField)field);
                  break;
               case SchemaType.Struct:
                  JToken vtStruct = jo.SelectToken(path);
                  value = CreateRow((JObject)vtStruct, ((StructField)field).Fields);
                  break;
               default:
                  throw new NotImplementedException();

            }

            values.Add(value);
         }

         return new Row(values);
      }

      private string GetJsonPath(Field field)
      {
         return "$." + field.Name;
      }

      private IList GetValues(JArray jArray, DataField df)
      {
         IList values = df.CreateEmptyList(true, false);

         foreach(JToken child in jArray.Children())
         {
            JValue jv = (JValue)child;
            object value = GetValue(jv.Value, df);
            values.Add(value);
         }

         return values;
      }

      private object GetValue(object jsonValue, DataField df)
      {
         if (jsonValue == null) return null;

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

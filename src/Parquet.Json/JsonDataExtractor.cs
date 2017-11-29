using System;
using System.Collections;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Parquet.Data;

namespace Parquet.Json
{
   public class JsonDataExtractor
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
         ds.Add(CreateRow(null, jo, _schema.Fields));
      }

      private Row CreateRow(string parentPath, JObject jo, IEnumerable<Field> fields)
      {
         var values = new List<object>();

         foreach (Field field in fields)
         {
            //string path = GetJsonPath(parentPath, field);
            string path = "$." + field.Name;
            object value;

            switch(field.SchemaType)
            {
               case SchemaType.Data:
                  JToken vt = jo.SelectToken(path);
                  if (vt == null)
                  {
                     value = null;
                  }
                  else
                  {
                     value = vt.Type == JTokenType.Array
                        ? GetValues((JArray) vt, (DataField) field)
                        : GetValue(((JValue) vt)?.Value, (DataField) field);
                  }
                  break;

               case SchemaType.Struct:
                  JToken vtStruct = jo.SelectToken(path);
                  value = CreateRow(path, (JObject)vtStruct, ((StructField)field).Fields);
                  break;

               case SchemaType.List:
                  JToken vtList = jo.SelectToken(path);
                  //it's always a struct in a list
                  ListField listField = (ListField)field;
                  StructField structField = (StructField) listField.Item;
                  var rows = new List<Row>();
                  foreach (JObject vtListItem in ((JArray) vtList).Children())
                  {
                     Row row = CreateRow(null, vtListItem, structField.Fields);
                     rows.Add(row);
                  }
                  value = rows;
                  break;

               default:
                  throw new NotImplementedException();

            }

            values.Add(value);
         }

         return new Row(values);
      }

      private string GetJsonPath(string parentPath, Field field)
      {
         return parentPath == null
            ? "$." + field.Name
            : parentPath + "." + field.Name;
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
            case DataType.Boolean:
               return new bool?((bool) jsonValue);
            case DataType.DateTimeOffset:
               return new DateTimeOffset?(new DateTimeOffset((DateTime)jsonValue));
            case DataType.Double:
               return new double?((double) jsonValue);
            default:
               throw new NotImplementedException($"conversion not implemented for type {df.DataType}");
         }
      }
   }
}

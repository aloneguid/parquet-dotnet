using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Parquet.Data;
using Parquet.Json.Data;

namespace Parquet.Json
{
   public class JsonSchemaInferring
   {
      private RelaxedField _root;
      private static readonly Dictionary<JTokenType, DataType> JsonTypeToParquetType = new Dictionary<JTokenType, DataType>
      {
         [JTokenType.Integer] = DataType.Int32,
         [JTokenType.Float] = DataType.Double,
         [JTokenType.String] = DataType.String,
         [JTokenType.Boolean] = DataType.Boolean,
         [JTokenType.Date] = DataType.DateTimeOffset,
         [JTokenType.Uri] = DataType.String,
      };

      //    JObject -> JContainer -> JToken
      //  JProperty -> JContainer
      //     JArray -> JContainer

      public JsonSchemaInferring()
      {
         _root = new RelaxedField("root", null);
      }

      private void Analyze(JObject jObject)
      {
         Analyze(_root, jObject);
      }

      private void Analyze(IEnumerable<JObject> jObjects)
      {
         JObject masterObject = null;
         var settings = new JsonMergeSettings
         {
            MergeNullValueHandling = MergeNullValueHandling.Ignore,
            MergeArrayHandling = MergeArrayHandling.Union
         };

         foreach (JObject jObject in jObjects)
         {
            if (masterObject == null)
            {
               masterObject = jObject;
            }
            else
            {
               masterObject.Merge(jObject, settings);
            }
         }

         Analyze(masterObject);
      }

      private void Analyze(RelaxedField parent, JToken token)
      {
         switch (token.Type)
         {
            //object has key-value properties that are columns!
            case JTokenType.Object:
               JObject jObject = (JObject)token;
               foreach (JToken child in jObject.Children())
               {
                  Analyze(parent, child);
               }
               break;

            //contains a value or further nesting
            case JTokenType.Property:
               JProperty jProperty = (JProperty)token;
               var property = new RelaxedField(jProperty.Name, parent);
               parent.Children.Add(property);
               Analyze(property, jProperty.Value);
               break;

            //is either a simple value or a list
            case JTokenType.Array:
               JArray jArray = (JArray)token;
               parent.IsArray = true;
               JToken jae = jArray.First;
               if (jae != null)
               {
                  if (jae.IsPrimitiveValue())
                  {
                     parent.DataType = GetParquetDataType(jae.Type, null);
                  }
                  else
                  {
                     //it's not an array then, children are actual objects!
                     Analyze(parent, jae);
                  }
               }
               else
               {
                  //array is empty, remove it from parent!
                  parent.Remove();
               }
               break;

            case JTokenType.Integer:
            case JTokenType.Float:
            case JTokenType.String:
            case JTokenType.Boolean:
            case JTokenType.Date:
            case JTokenType.Raw:
            case JTokenType.Bytes:
            case JTokenType.Guid:
            case JTokenType.Uri:
            case JTokenType.TimeSpan:
               parent.DataType = GetParquetDataType(token.Type, null);
               break;

            case JTokenType.Null:
               //cannot infer type for nulls
               parent.Remove();
               break;

            default:
               throw new NotImplementedException($"what's {token.Type}?");
         }
      }

      public Schema InferSchema(IEnumerable<JObject> jObjects)
      {
         Analyze(jObjects);

         return new Schema(_root.Children.Select(c => c.ToField()));
      }


      private DataType GetParquetDataType(JTokenType jType, DataType? existingType)
      {
         if(!JsonTypeToParquetType.TryGetValue(jType, out DataType pt))
         {
            throw new NotImplementedException($"{jType} is not mapped");
         }

         return pt;
      }

   }
}

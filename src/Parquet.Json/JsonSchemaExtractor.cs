using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using Parquet.Data;
using Parquet.Json.Data;

namespace Parquet.Json
{
   class JsonSchemaExtractor
   {
      private RelaxedField _root;
      private static readonly Dictionary<JTokenType, DataType> JsonTypeToParquetType = new Dictionary<JTokenType, DataType>
      {
         [JTokenType.Integer] = DataType.Int32,
         [JTokenType.Float] = DataType.Float,
         [JTokenType.String] = DataType.String,
         [JTokenType.Boolean] = DataType.Boolean,
         [JTokenType.Null] = DataType.Unspecified,
         [JTokenType.Date] = DataType.DateTimeOffset,
         [JTokenType.Raw] = DataType.ByteArray,
         [JTokenType.Bytes] = DataType.ByteArray,
         [JTokenType.Guid] = DataType.ByteArray,
         [JTokenType.Uri] = DataType.String,
         [JTokenType.TimeSpan] = DataType.Interval
      };

      //    JObject -> JContainer -> JToken
      //  JProperty -> JContainer
      //     JArray -> JContainer

      public JsonSchemaExtractor()
      {
         _root = new RelaxedField("root", null);
      }

      //todo: merge all incoming JObjects
      public void Analyze(IEnumerable<JObject> jObjects)
      {
         
      }

      public void Analyze(JObject jObject)
      {
         Analyze(_root, jObject);
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
               JToken jae = jArray.First;
               if (jae != null)
               {
                  if (jae.IsPrimitiveValue())
                  {
                     parent.IsArray = true;
                     parent.DataType = GetParquetDataType(jae.Type, null);
                  }
                  else
                  {
                     //it's not an array then, children are actual objects!
                     Analyze(parent, jae);
                  }
               }
               break;

            case JTokenType.Integer:
            case JTokenType.String:
               parent.DataType = GetParquetDataType(token.Type, null);
               break;

            default:
               throw new NotImplementedException($"what's {token.Type}?");
         }
      }

      public Schema GetSchema()
      {
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

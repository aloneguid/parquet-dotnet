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

      public JsonSchemaExtractor()
      {
         _root = new RelaxedField("root", null);
      }

      public void Analyze(JObject jObject)
      {
         Analyze(_root, jObject);
      }

      public Schema GetSchema()
      {
         return new Schema(_root.Children.Select(c => c.ToField()));
      }

      public void Analyze(RelaxedField parent, JContainer jo)
      {
         foreach (JToken jt in jo)
         {
            switch (jt.Type)
            {
               case JTokenType.Property:
                  AnalyzeProperty(parent, jt as JProperty);
                  break;
               default:
                  throw new NotImplementedException($"no {jt.Type} yet ;)");

            }
         }
      }

      private void AnalyzeProperty(RelaxedField parent, JProperty jp)
      {
         if(jp.Value.Type >= JTokenType.Integer)
         {
            var field = new RelaxedField(jp.Name, parent);
            field.DataType = GetParquetDataType(jp.Value.Type, parent.DataType);
            parent.Children.Add(field);
         }
         else if(jp.Value.Type == JTokenType.Object)
         {
            var field = new RelaxedField(jp.Name, parent);
            parent.Children.Add(field);
            Analyze(field, (JContainer)jp.Value);
         }
         else
         {
            throw new NotImplementedException();
         }
      }

      private DataType GetParquetDataType(JTokenType jType, DataType? existingType)
      {
         return JsonTypeToParquetType[jType];
      }

   }
}

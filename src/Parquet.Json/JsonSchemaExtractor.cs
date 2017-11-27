using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json.Linq;
using Parquet.Data;
using Parquet.Json.Data;

namespace Parquet.Json
{
   class JsonSchemaExtractor
   {
      private RelaxedField _root;

      public JsonSchemaExtractor()
      {
         _root = new RelaxedField("root");
      }

      public void Analyze(JObject jObject)
      {
         Analyze(_root, jObject);
      }

      public void Analyze(RelaxedField parent, JToken jToken)
      {
         foreach(JToken token in jToken.Children())
         {
            switch(token.Type)
            {
               case JTokenType.Property:
                  Analyze(parent, (JProperty)token);
                  break;

               case JTokenType.Object:
                  Analyze(parent, (JObject)token);
                  break;

               default:
                  throw new NotSupportedException($"unsupported token type: {token.Type}");
            }
         }
      }

      private void Analyze(RelaxedField parent, JProperty property)
      {
         var f = new RelaxedField(property.Name);
         parent.Children.Add(f);
         Analyze(f, property.Value);
      }

      private void Analyze(RelaxedField parent, JObject jObject)
      {
         parent = null;
      }
   }
}

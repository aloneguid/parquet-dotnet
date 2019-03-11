using System;
using System.Collections;
using System.Text;
using Parquet.Data;
using Parquet.Data.Rows;
using Parquet.Serialization;

namespace Parquet.Extensions
{
   internal static class StringBuilderExtensions
   {
      private const string BraceOpen = "{";
      private const string BraceClose = "}";
      private const string JsonQuote = "\"";
      private const string JsonSingleQuote = "'";

      public static void StartArray(this StringBuilder sb, StringFormat sf, int level)
      {
         if(level > 0)
         {
            sb.Append("[");
         }
      }

      public static void EndArray(this StringBuilder sb, StringFormat sf, int level)
      {
         if(level > 0)
         {
            sb.Append("]");
         }
      }

      public static void DivideObjects(this StringBuilder sb, StringFormat sf, int level)
      {
         if (level > 0)
         {
            switch (sf)
            {
               case StringFormat.Json:
                  sb.Append(",");
                  break;
               default:
                  sb.Append(", ");
                  break;
            }
         }
         else
         {
            sb.AppendLine();
         }         
      }

      public static void StartObject(this StringBuilder sb, StringFormat sf)
      {
         sb.Append("{");
      }

      public static void EndObject(this StringBuilder sb, StringFormat sf)
      {
         sb.Append("}");
      }

      public static void AppendPropertyName(this StringBuilder sb, StringFormat sf, Field f)
      {
         switch (sf)
         {
            case StringFormat.Json:
               if (f != null)
               {
                  sb.Append(JsonQuote);
                  sb.Append(f?.Name ?? "?");
                  sb.Append(JsonQuote);
                  sb.Append(":");
               }
               break;
            case StringFormat.JsonSingleQuote:
               if (f != null)
               {
                  sb.Append(JsonSingleQuote);
                  sb.Append(f?.Name ?? "?");
                  sb.Append(JsonSingleQuote);
                  sb.Append(": ");
               }
               break;
         }
      }

      public static void AppendNull(this StringBuilder sb, StringFormat sf)
      {
         sb.Append("null");
      }

      public static void Append(this StringBuilder sb, StringFormat sf, object value)
      {
         EncodeJson(sb, sf, value);
      }

      private static void EncodeJson(StringBuilder sb, StringFormat sf, object value)
      {
         if (value == null)
         {
            AppendNull(sb, sf);
            return;
         }

         Type t = value.GetType();
         string quote = sf == StringFormat.Json ? JsonQuote : JsonSingleQuote;

         if (t == typeof(string))
         {
            sb.Append(quote);
            sb.Append(HttpEncoder.JavaScriptStringEncode((string)value));
            sb.Append(quote);
         }
         else if(t == typeof(DateTimeOffset))
         {
            sb.Append(quote);
            sb.Append(value.ToString());
            sb.Append(quote);
         }
         else if(t == typeof(bool))
         {
            sb.Append((bool)value ? "true" : "false");
         }
         else
         {
            sb.Append(value.ToString());
         }
      }
   }
}

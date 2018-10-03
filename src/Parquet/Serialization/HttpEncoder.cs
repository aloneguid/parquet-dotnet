using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Parquet.Serialization
{
   class HttpEncoder
   {
      private const bool JavaScriptEncodeAmpersand = false;

      /// <summary>Encodes a string.</summary>
      /// <param name="value">The string to encode.</param>
      /// <returns>An encoded string.</returns>
      public static string JavaScriptStringEncode(string value)
      {
         if (string.IsNullOrEmpty(value))
            return string.Empty;
         StringBuilder builder = (StringBuilder)null;
         int startIndex = 0;
         int count = 0;
         for (int index = 0; index < value.Length; ++index)
         {
            char c = value[index];
            if (CharRequiresJavaScriptEncoding(c))
            {
               if (builder == null)
                  builder = new StringBuilder(value.Length + 5);
               if (count > 0)
                  builder.Append(value, startIndex, count);
               startIndex = index + 1;
               count = 0;
            }
            switch (c)
            {
               case '\b':
                  builder.Append("\\b");
                  break;
               case '\t':
                  builder.Append("\\t");
                  break;
               case '\n':
                  builder.Append("\\n");
                  break;
               case '\f':
                  builder.Append("\\f");
                  break;
               case '\r':
                  builder.Append("\\r");
                  break;
               case '"':
                  builder.Append("\\\"");
                  break;
               case '\\':
                  builder.Append("\\\\");
                  break;
               default:
                  if (CharRequiresJavaScriptEncoding(c))
                  {
                     AppendCharAsUnicodeJavaScript(builder, c);
                     break;
                  }
                  ++count;
                  break;
            }
         }
         if (builder == null)
            return value;
         if (count > 0)
            builder.Append(value, startIndex, count);
         return builder.ToString();
      }

      private static bool CharRequiresJavaScriptEncoding(char c)
      {
         if (c >= ' ' && c != '"' && (c != '\\' && c != '\'') && (c != '<' && c != '>' && (c != '&' || !JavaScriptEncodeAmpersand)) && (c != '\x0085' && c != '\x2028'))
            return c == '\x2029';
         return true;
      }

      private static void AppendCharAsUnicodeJavaScript(StringBuilder builder, char c)
      {
         builder.Append("\\u");
         builder.Append(((int)c).ToString("x4", (IFormatProvider)CultureInfo.InvariantCulture));
      }

   }
}
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using Parquet.Extensions;

namespace Parquet.Data.Rows
{
   /// <summary>
   /// Represents a tabular row
   /// </summary>
   public class Row : IEquatable<Row>, IEnumerable<object>
   {
      /// <summary>
      /// Initializes a new instance of the <see cref="Row"/> class which has only one single column.
      /// </summary>
      public Row(object value) : this(new[] { value })
      {

      }

      //eventually we can expose this externally, however it's not ready yet.
      internal Field[] Schema { get; set; }

      /// <summary>
      /// Creates a single cell row. Use this method to avoid overloading confusion.
      /// </summary>
      public static Row SingleCell(object value)
      {
         return new Row(value);
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="Row"/> class.
      /// </summary>
      public Row(IEnumerable<object> values)
      {
         Values = values.ToArray();
      }

      internal Row(IReadOnlyCollection<Field> schema, IEnumerable<object> values) : this(values)
      {
         Schema = schema?.ToArray();
      }

      /// <summary>
      /// Initializes a new instance of the <see cref="Row"/> class.
      /// </summary>
      public Row(params object[] values)
      {
         Values = values;
      }

      /// <summary>
      /// Raw values
      /// </summary>
      public object[] Values { get; internal set; }

      /// <summary>
      /// Gets the number of values in this row
      /// </summary>
      public int Length => Values.Length;

      /// <summary>
      /// Gets the row value by index
      /// </summary>
      public object this[int i]
      {
         get
         {
            return Values[i];
         }
         set
         {
            Values[i] = value;
         }
      }

      /// <summary>
      /// Gets the value as boolean
      /// </summary>
      /// <param name="i">Value index</param>
      public bool GetBoolean(int i)
      {
         return Get<bool>(i);
      }
      /// <summary>
      /// Gets the value as integer
      /// </summary>
      /// <param name="i">Value index</param>
      public int GetInt(int i)
      {
         return Get<int>(i);
      }

      /// <summary>
      /// Gets the value as float
      /// </summary>
      /// <param name="i">Value index</param>
      public float GetFloat(int i)
      {
         return Get<float>(i);
      }

      /// <summary>
      /// Gets the value as long
      /// </summary>
      /// <param name="i">Value index</param>
      public long GetLong(int i)
      {
         return Get<long>(i);
      }

      /// <summary>
      /// Gets the value as double
      /// </summary>
      /// <param name="i">Value index</param>
      public double GetDouble(int i)
      {
         return Get<double>(i);
      }

      /// <summary>
      /// Gets the value as big integer
      /// </summary>
      /// <param name="i">Value index</param>
      public BigInteger GetBigInt(int i)
      {
         return Get<BigInteger>(i);
      }

      /// <summary>
      /// Gets the value as byte array
      /// </summary>
      /// <param name="i">Value index</param>
      public byte[] GetByteArray(int i)
      {
         return Get<byte[]>(i);
      }

      /// <summary>
      /// Gets the value as string
      /// </summary>
      /// <param name="i">Value index</param>
      public string GetString(int i)
      {
         return Get<string>(i);
      }

      /// <summary>
      /// Gets the value as <see cref="DateTimeOffset"/>
      /// </summary>
      /// <param name="i">Value index</param>
      public DateTimeOffset GetDateTimeOffset(int i)
      {
         return Get<DateTimeOffset>(i);
      }

      /// <summary>
      /// Returns true if value at column <paramref name="i"/> is NULL.
      /// </summary>
      public bool IsNullAt(int i)
      {
         return Values[i] == null;
      }

      /// <summary>
      /// Gets the value trying to cast to <typeparamref name="T"/>
      /// </summary>
      /// <param name="i">Value index</param>
      /// <exception cref="ArgumentException">Cannot cast <typeparamref name="T"/></exception>
      public T Get<T>(int i)
      {
         object v = Values[i];

         if (v == null)
            return default(T);

         if (!(v is T))
         {
            throw new ArgumentException($"value at {i} is of type '{v.GetType()}' and cannot be casted to '{typeof(T)}'");
         }

         return (T)v;
      }

      /// <summary>
      /// Converts to internal format string
      /// </summary>
      /// <returns></returns>
      public override string ToString()
      {
         return ToString(null);
      }

      /// <summary>
      /// Convert to string with optional formatting
      /// </summary>
      /// <param name="format">jsq - one line single-quote json, default, j - one line json</param>
      public string ToString(string format)
      {
         var sb = new StringBuilder();
         
         ToString(sb, GetStringFormat(format), 1, Schema);

         return sb.ToString();
      }

      internal static StringFormat GetStringFormat(string format)
      {
         StringFormat sf;
         if (format == "j")
            sf = StringFormat.Json;
         else
            sf = StringFormat.JsonSingleQuote;
         return sf;
      }

      internal void ToString(StringBuilder sb, StringFormat sf, int level, IReadOnlyCollection<Field> fields)
      {
         ToString(sb, Values, sf, level, fields);
      }

      internal void ToString(StringBuilder sb, object[] values, StringFormat sf, int level, IReadOnlyCollection<Field> fields)
      {
         if (fields == null)
         {
            sb.AppendFormat("{0} value(s)", values?.Length ?? 0);
            return;
         }

         sb.StartObject(sf);

         bool first = true;
         int nl = level + 1;
         foreach (Tuple<object, Field> valueWithField in values.IterateWith(fields))
         {
            object v = valueWithField.Item1;
            Field f = valueWithField.Item2;

            if (first)
            {
               first = false;
            }
            else
            {
               sb.DivideObjects(sf, level);
            }

            FormatValue(v, sb, sf, f, nl);
         }

         sb.EndObject(sf);
      }

      private void FormatValue(object v, StringBuilder sb, StringFormat sf, Field f, int level, bool appendPropertyName = true)
      {
         if (appendPropertyName)
         {
            sb.AppendPropertyName(sf, f);
         }

         bool first = true;

         if (v == null)
         {
            sb.AppendNull(sf);
         }
         else if(f != null)
         {
            switch (f.SchemaType)
            {
               case SchemaType.Data:
                  DataField df = (DataField)f;
                  if(df.IsArray)
                  {
                     sb.StartArray(sf, level);
                     foreach(object vb in (IEnumerable)v)
                     {
                        if (first)
                           first = false;
                        else
                        {
                           sb.DivideObjects(sf, level);
                        }

                        sb.Append(sf, vb);
                     }
                     sb.EndArray(sf, level);
                  }
                  else
                  {
                     sb.Append(sf, v);
                  }
                  break;

               case SchemaType.Struct:
                  StructField stf = (StructField)f;
                  if (!(v is Row sRow))
                  {
                     //throw new FormatException($"expected {typeof(Row)} at {f.Path} but found {v.GetType()}");
                  }
                  else
                  {
                     ToString(sb, sRow.Values, sf, level + 1, stf.Fields);
                  }
                  break;

               case SchemaType.Map:
                  MapField mf = (MapField)f;
                  sb.StartArray(sf, level);
                  foreach(Row row in (IEnumerable)v)
                  {
                     if (first)
                        first = false;
                     else
                        sb.DivideObjects(sf, level);

                     ToString(sb, row.Values, sf, level + 1, new[] { mf.Key, mf.Value });
                  }
                  sb.EndArray(sf, level);
                  break;

               case SchemaType.List:
                  ListField lf = (ListField)f;
                  sb.StartArray(sf, level);
                  foreach(object le in (IEnumerable)v)
                  {
                     if (first)
                        first = false;
                     else
                        sb.DivideObjects(sf, level);

                     FormatValue(le, sb, sf, lf.Item, level + 1, false);
                  }
                  sb.EndArray(sf, level);
                  break;

               default:
                  throw new NotImplementedException(f.SchemaType.ToString());
            }
         }
         else
         {
            throw new NotImplementedException("null schema, value: " + v);
         }
      }

      private static IReadOnlyCollection<Field> GetMoreFields(Field f)
      {
         if (f == null)
            return null;

         switch (f.SchemaType)
         {
            case SchemaType.List:
               return new[] { ((ListField)f).Item };

            case SchemaType.Map:
               MapField mf = (MapField)f;
               return new[] { mf.Key, mf.Value };

            case SchemaType.Struct:
               return ((StructField)f).Fields;

            default:
               return null;
         }
      }

      /// <summary>
      /// 
      /// </summary>
      public bool Equals(Row other)
      {
         return Equals(other, false);
      }

      /// <summary>
      /// 
      /// </summary>
      public bool Equals(Row other, bool throwException)
      {
         if (Values.Length != other.Values.Length)
         {
            if(throwException)
            {
               throw new ArgumentException($"values count is different ({Values.Length} != {other.Values.Length})");
            }

            return false;
         }

         for(int i = 0; i < Values.Length; i++)
         {
            object v = Values[i];
            object ov = other.Values[i];

            if(!Equal(v, ov, i, throwException))
            {
               return false;
            }
         }

         return true;
      }

      private bool Equal(object v, object ov, int position, bool throwException)
      {
         if (v == null || ov == null)
         {
            bool equal = v == null && ov == null;

            if (!equal && throwException)
            {
               throw new ArgumentException($"only one of the values is null at position {position}");
            }

            return equal;
         }

         if (v.GetType().IsArray && ov.GetType().IsArray)
         {
            bool equal = ((Array)v).EqualTo((Array)ov);

            if (!equal && throwException)
            {
               throw new ArgumentException($"arrays at position {position} are not equal");
            }

            return equal;
         }

         if (!v.Equals(ov))
         {
            if (throwException)
            {
               throw new ArgumentException($"values are not equal at position {position} ({v} != {ov})");
            }

            return false;
         }

         return true;
      }

      /// <summary>
      /// Gets object enumerator
      /// </summary>
      public IEnumerator<object> GetEnumerator() => ((IEnumerable<object>)Values).GetEnumerator();

      IEnumerator IEnumerable.GetEnumerator() => Values.GetEnumerator();
   }
}
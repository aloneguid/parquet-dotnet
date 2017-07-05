/* MIT License
 *
 * Copyright (c) 2017 Elastacloud Limited
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

using Parquet.Thrift;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using Type = System.Type;
using TType = Parquet.Thrift.Type;
using Parquet.File;

namespace Parquet
{
   /// <summary>
   /// Represents a column
   /// </summary>
   /// <typeparam name="T"></typeparam>
   public class ParquetColumn<T> : ParquetColumn
   {
      /// <summary>
      /// Creates an instance of the column by name
      /// </summary>
      /// <param name="name">Column name</param>
      public ParquetColumn(string name) : base(name, typeof(T))
      {

      }

      /// <summary>
      /// Adds values to this column
      /// </summary>
      /// <param name="values"></param>
      public void Add(params T[] values)
      {
         base.Add(values);
      }
   }

   /// <summary>
   /// Represents a column
   /// </summary>
   public class ParquetColumn : IEquatable<ParquetColumn>
   {
      private readonly SchemaElement _schema;

      /// <summary>
      /// Creates a column from name and type
      /// </summary>
      /// <param name="name">Column name</param>
      /// <param name="systemType">Data type to be held in this column, can be nullable</param>
      public ParquetColumn(string name, Type systemType)
      {
         Name = name ?? throw new ArgumentNullException(nameof(name));
         _schema = new SchemaElement(name)
         {
            Repetition_type = FieldRepetitionType.REQUIRED
         };
         Values = ListFactory.Create(systemType, _schema);
         SystemType = systemType;
      }

      internal ParquetColumn(string name, SchemaElement schema)
      {
         Name = name ?? throw new ArgumentNullException(nameof(name));
         _schema = schema ?? throw new ArgumentNullException(nameof(schema));
         Values = CreateValuesList(schema, out Type systemType);
         SystemType = systemType;
      }

      /// <summary>
      /// Column name
      /// </summary>
      public string Name { get; }

      /// <summary>
      /// System type representing items in the list
      /// </summary>
      public Type SystemType { get; }

      /// <summary>
      /// Parquet type as read from schema
      /// </summary>
      public string ParquetRawType => _schema.Type.ToString();

      internal TType Type => _schema.Type;

      /// <summary>
      /// List of values
      /// </summary>
      public IList Values { get; private set; }

      internal SchemaElement Schema => _schema;

      /// <summary>
      /// Adds values
      /// </summary>
      /// <param name="values"></param>
      public void Add(params object[] values)
      {
         Add(values);
      }

      /// <summary>
      /// Merges values from the passed list into this column
      /// </summary>
      /// <param name="values"></param>
      public void Add(IList values)
      {
         //todo: if properly casted speed will increase
         foreach (var value in values)
         {
            Values.Add(value);
         }
      }

      internal void Assign(IList values)
      {
         Values.Clear();
         if (values == null) return;

         //todo: can we improve this unboxing somehow?
         foreach(object value in values)
         {
            Values.Add(value);
         }
      }

      /// <summary>
      /// Returns column name
      /// </summary>
      /// <returns></returns>
      public override string ToString()
      {
         return Name;
      }

      /// <summary>
      /// Equals override
      /// </summary>
      public bool Equals(ParquetColumn other)
      {
         if (ReferenceEquals(other, null)) return false;
         if (ReferenceEquals(other, this)) return true;
         return other.Name == this.Name;
      }

      /// <summary>
      /// Equals override
      /// </summary>
      public override bool Equals(object obj)
      {
         if (ReferenceEquals(obj, null)) return false;
         if (obj.GetType() != typeof(ParquetColumn)) return false;
         return Equals((ParquetColumn)obj);
      }

      /// <summary>
      /// GetHashCode override
      /// </summary>
      public override int GetHashCode()
      {
         return Name.GetHashCode();
      }

      internal static IList CreateValuesList(SchemaElement schema, out Type systemType)
      {
         switch(schema.Type)
         {
            case TType.BOOLEAN:
               if (schema.Repetition_type == FieldRepetitionType.OPTIONAL)
               {
                  systemType = typeof(bool?);
                  return new List<bool?>();
               }
               systemType = typeof(bool);
               return new List<bool>();
            case TType.INT32:
               if(schema.Converted_type == ConvertedType.DATE)
               {
                  if (schema.Repetition_type == FieldRepetitionType.OPTIONAL)
                  {
                     systemType = typeof(DateTimeOffset?);
                     return new List<DateTimeOffset?>();
                  }
                  systemType = typeof(DateTimeOffset);
                  return new List<DateTimeOffset>();
               }
               else
               {
                  if (schema.Repetition_type == FieldRepetitionType.OPTIONAL)
                  {
                     systemType = typeof(int?);
                     return new List<int?>();
                  }
                  systemType = typeof(int);
                  return new List<int>();
               }
            case TType.FLOAT:
               if (schema.Repetition_type == FieldRepetitionType.OPTIONAL)
               {
                  systemType = typeof(float?);
                  return new List<float?>();
               }
               systemType = typeof(float);
               return new List<float>();
            case TType.INT64:
               if (schema.Repetition_type == FieldRepetitionType.OPTIONAL)
               {
                  systemType = typeof(long?);
                  return new List<long?>();
               }
               systemType = typeof(long);
               return new List<long>();
            case TType.DOUBLE:
               if (schema.Repetition_type == FieldRepetitionType.OPTIONAL)
               {
                  systemType = typeof(double?);
                  return new List<double?>();
               }
               systemType = typeof(double);
               return new List<double>();
            case TType.INT96:
#if !SPARK_TYPES
               systemType = typeof(DateTimeOffset?);
               return new List<DateTimeOffset?>();
#else
               systemType = typeof(BigInteger?);
               return (new List<BigInteger?>(), new List<BigInteger?>());
#endif
            case TType.BYTE_ARRAY:
               if(schema.Converted_type == ConvertedType.UTF8)
               {
                  systemType = typeof(string);
                  return new List<string>();
               }
               else
               {
                  if (schema.Repetition_type == FieldRepetitionType.OPTIONAL)
                  {
                     systemType = typeof(bool?);
                     return new List<bool?>();
                  }
                  systemType = typeof(bool);
                  return new List<bool>();
               }
            case TType.FIXED_LEN_BYTE_ARRAY:
               // TODO: Converted type should work differently shouldn't inline in this way
               if (schema.Converted_type == ConvertedType.DECIMAL)
               {
                  if (schema.Repetition_type == FieldRepetitionType.OPTIONAL)
                  {
                     systemType = typeof(decimal?);
                     return new List<decimal?>();
                  }
                  systemType = typeof(decimal);
                  return new List<decimal>();
               }
               else
               {
                  if (schema.Repetition_type == FieldRepetitionType.OPTIONAL)
                  {
                     systemType = typeof(byte?[]);
                     return new List<byte?[]>();
                  }
                  systemType = typeof(byte[]);
                  return new List<byte[]>();
               }
            default:
               throw new NotImplementedException($"type {schema.Type} not implemented");
         }
      }
   }
}
// DbDataReader isn't included in netstandard 1.4 or 1.6.
#if NETSTANDARD2_0_OR_GREATER || NETCOREAPP1_0_OR_GREATER

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.IO;
using Parquet.Data;

namespace Parquet.Extensions
{
   /// <summary>
   /// Extensions for interoperating with ADO.NET DbDataReader.
   /// </summary>
   public static class DataReaderExtensions
   {
      struct ColumnInfo
      {
         public string name;
         public Type type;
         public TypeCode typeCode;
         public bool allowNull;
         public DataField field;
         public ColumnHandler writer;
         public Array dataArray;
      }

      abstract class ColumnHandler
      {
         public abstract Type ElementType { get; }
         public abstract Array CreateDataArray(int length);
         public abstract void Process(DbDataReader reader, int ordinal, Array dataArray, int idx);

         public static ColumnHandler String = new StringColumnHandler();
         public static ColumnHandler Binary = new BinaryColumnHandler();
         public static ColumnHandler Bool = new BoolColumnHandler();
         public static ColumnHandler NullableBool = new NullableBoolColumnHandler();
         public static ColumnHandler Byte = new ByteColumnHandler();
         public static ColumnHandler NullableByte = new NullableByteColumnHandler();
         public static ColumnHandler Int16 = new Int16ColumnHandler();
         public static ColumnHandler NullableInt16 = new NullableInt16ColumnHandler();
         public static ColumnHandler Int32 = new Int32ColumnHandler();
         public static ColumnHandler NullableInt32 = new NullableInt32ColumnHandler();
         public static ColumnHandler Int64 = new Int64ColumnHandler();
         public static ColumnHandler NullableInt64 = new NullableInt64ColumnHandler();
         public static ColumnHandler Float = new FloatColumnHandler();
         public static ColumnHandler NullableFloat = new NullableFloatColumnHandler();
         public static ColumnHandler Double = new DoubleColumnHandler();
         public static ColumnHandler NullableDouble = new NullableDoubleColumnHandler();
         public static ColumnHandler Decimal = new DecimalColumnHandler();
         public static ColumnHandler NullableDecimal = new NullableDecimalColumnHandler();
         public static ColumnHandler DateTime = new DateTimeColumnHandler();
         public static ColumnHandler NullableDateTime = new NullableDateTimeColumnHandler();
      }

      class ColumnHandler<T> : ColumnHandler
      {
         public override Type ElementType => typeof(T);

         public override Array CreateDataArray(int length)
         {
            return CreateArray(length);
         }

         T[] CreateArray(int length)
         {
            return new T[length];
         }

         public override void Process(DbDataReader reader, int ordinal, Array dataArray, int idx)
         {
            ((T[])dataArray)[idx] = GetValue(reader, ordinal);
         }

         public virtual T GetValue(DbDataReader reader, int ordinal)
         {
            return reader.GetFieldValue<T>(ordinal);
         }
      }

      class StringColumnHandler : ColumnHandler<string>
      {
         public override string GetValue(DbDataReader reader, int ordinal)
         {
            return
                reader.IsDBNull(ordinal)
                ? null
                : reader.GetString(ordinal);
         }
      }

      sealed class BinaryColumnHandler : ColumnHandler<byte[]>
      {
         public override byte[] GetValue(DbDataReader reader, int ordinal)
         {
            if (reader.IsDBNull(ordinal))
               return null;
            // TODO: Not sure how reliably this is implemented/supported vs GetBytes.
            return (byte[])reader.GetValue(ordinal);
         }
      }

      sealed class BoolColumnHandler : ColumnHandler<bool>
      {
         public override bool GetValue(DbDataReader reader, int ordinal)
         {
            return reader.GetBoolean(ordinal);
         }
      }

      sealed class NullableBoolColumnHandler : ColumnHandler<bool?>
      {
         public override bool? GetValue(DbDataReader reader, int ordinal)
         {
            if (reader.IsDBNull(ordinal))
               return null;
            return reader.GetBoolean(ordinal);
         }
      }

      sealed class ByteColumnHandler : ColumnHandler<byte>
      {
         public override byte GetValue(DbDataReader reader, int ordinal)
         {
            return reader.GetByte(ordinal);
         }
      }

      sealed class NullableByteColumnHandler : ColumnHandler<byte?>
      {
         public override byte? GetValue(DbDataReader reader, int ordinal)
         {
            if (reader.IsDBNull(ordinal))
               return null;
            return reader.GetByte(ordinal);
         }
      }

      sealed class Int16ColumnHandler : ColumnHandler<short>
      {
         public override short GetValue(DbDataReader reader, int ordinal)
         {
            return reader.GetInt16(ordinal);
         }
      }

      sealed class NullableInt16ColumnHandler : ColumnHandler<short?>
      {
         public override short? GetValue(DbDataReader reader, int ordinal)
         {
            if (reader.IsDBNull(ordinal))
               return null;
            return reader.GetInt16(ordinal);
         }
      }

      sealed class Int32ColumnHandler : ColumnHandler<int>
      {
         public override int GetValue(DbDataReader reader, int ordinal)
         {
            return reader.GetInt32(ordinal);
         }
      }

      sealed class NullableInt32ColumnHandler : ColumnHandler<int?>
      {
         public override int? GetValue(DbDataReader reader, int ordinal)
         {
            if (reader.IsDBNull(ordinal))
               return null;
            return reader.GetInt32(ordinal);
         }
      }

      sealed class Int64ColumnHandler : ColumnHandler<long>
      {
         public override long GetValue(DbDataReader reader, int ordinal)
         {
            return reader.GetInt64(ordinal);
         }
      }

      sealed class NullableInt64ColumnHandler : ColumnHandler<long?>
      {
         public override long? GetValue(DbDataReader reader, int ordinal)
         {
            if (reader.IsDBNull(ordinal))
               return null;
            return reader.GetInt64(ordinal);
         }
      }

      sealed class FloatColumnHandler : ColumnHandler<float>
      {
         public override float GetValue(DbDataReader reader, int ordinal)
         {
            return reader.GetFloat(ordinal);
         }
      }

      sealed class NullableFloatColumnHandler : ColumnHandler<float?>
      {
         public override float? GetValue(DbDataReader reader, int ordinal)
         {
            if (reader.IsDBNull(ordinal))
               return null;
            return reader.GetFloat(ordinal);
         }
      }

      sealed class DoubleColumnHandler : ColumnHandler<double>
      {
         public override double GetValue(DbDataReader reader, int ordinal)
         {
            return reader.GetDouble(ordinal);
         }
      }

      sealed class NullableDoubleColumnHandler : ColumnHandler<double?>
      {
         public override double? GetValue(DbDataReader reader, int ordinal)
         {
            if (reader.IsDBNull(ordinal))
               return null;
            return reader.GetDouble(ordinal);
         }
      }

      sealed class DecimalColumnHandler : ColumnHandler<decimal>
      {
         public override decimal GetValue(DbDataReader reader, int ordinal)
         {
            return reader.GetDecimal(ordinal);
         }
      }

      sealed class NullableDecimalColumnHandler : ColumnHandler<decimal?>
      {
         public override decimal? GetValue(DbDataReader reader, int ordinal)
         {
            if (reader.IsDBNull(ordinal))
               return null;
            return reader.GetDecimal(ordinal);
         }
      }

      sealed class DateTimeColumnHandler : ColumnHandler<DateTimeOffset>
      {
         public override DateTimeOffset GetValue(DbDataReader reader, int ordinal)
         {
            return reader.GetDateTime(ordinal);
         }
      }

      sealed class NullableDateTimeColumnHandler : ColumnHandler<DateTimeOffset?>
      {
         public override DateTimeOffset? GetValue(DbDataReader reader, int ordinal)
         {
            if (reader.IsDBNull(ordinal))
               return null;
            return reader.GetDateTime(ordinal);
         }
      }

      /// <summary>
      /// Writes the data from a DbDataReader as a parquet stream.
      /// </summary>
      /// <param name="reader">The DbDataReader containing the data to write.</param>
      /// <param name="filename">The name of the file to write to.</param>
      /// <param name="rowGroupSize">The size of the parquet row groups to write.</param>
      public static void WriteParquet(this DbDataReader reader, string filename, int rowGroupSize = 0x10000)
      {
         using Stream stream = System.IO.File.Create(filename);
         WriteParquet(reader, stream, rowGroupSize);
      }

      /// <summary>
      /// Writes the data from a DbDataReader as a parquet stream.
      /// </summary>
      /// <param name="reader">The DbDataReader containing the data to write.</param>
      /// <param name="stream">The streamto write to.</param>
      /// <param name="rowGroupSize">The size of the parquet row groups to write.</param>
      public static void WriteParquet(this DbDataReader reader, Stream stream, int rowGroupSize = 0x10000)
      {
         ReadOnlyCollection<DbColumn> colSchema = reader.GetColumnSchema();
         var fields = new List<Field>();
         ColumnInfo[] columnInfos = new ColumnInfo[colSchema.Count];

         for (int i = 0; i < colSchema.Count; i++)
         {
            DbColumn col = colSchema[i];
            Type dataType = col.DataType;

            bool allowsNull = col.AllowDBNull ?? true;
            TypeCode typeCode = Type.GetTypeCode(dataType);
            ColumnHandler colWriter;
            switch (typeCode)
            {
               case TypeCode.String:
                  colWriter = ColumnHandler.String;
                  break;
               case TypeCode.Boolean:
                  colWriter = allowsNull ? ColumnHandler.NullableBool : ColumnHandler.Bool;
                  break;
               case TypeCode.Byte:
                  colWriter = allowsNull ? ColumnHandler.NullableByte : ColumnHandler.Byte;
                  break;
               case TypeCode.Int16:
                  colWriter = allowsNull ? ColumnHandler.NullableInt16 : ColumnHandler.Int16;
                  break;
               case TypeCode.Int32:
                  colWriter = allowsNull ? ColumnHandler.NullableInt32 : ColumnHandler.Int32;
                  break;
               case TypeCode.Int64:
                  colWriter = allowsNull ? ColumnHandler.NullableInt64 : ColumnHandler.Int64;
                  break;
               case TypeCode.Single:
                  colWriter = allowsNull ? ColumnHandler.NullableFloat : ColumnHandler.Float;
                  break;
               case TypeCode.Double:
                  colWriter = allowsNull ? ColumnHandler.NullableDouble : ColumnHandler.Double;
                  break;
               case TypeCode.Decimal:
                  colWriter = allowsNull ? ColumnHandler.NullableDecimal : ColumnHandler.Decimal;
                  break;
               case TypeCode.DateTime:
                  colWriter = allowsNull ? ColumnHandler.NullableDateTime : ColumnHandler.DateTime;
                  break;
               default:
                  if (dataType == typeof(byte[]))
                  {
                     colWriter = ColumnHandler.Binary;
                     break;
                  }
                  throw new NotSupportedException();
            }
            var f = new DataField(col.ColumnName, colWriter.ElementType);
            fields.Add(f);

            columnInfos[i] =
               new ColumnInfo
               {
                  name = col.ColumnName,
                  type = dataType,
                  typeCode = typeCode,
                  allowNull = allowsNull,
                  field = f,
                  writer = colWriter,
                  dataArray = colWriter.CreateDataArray(rowGroupSize),
               };
         }

         var schema = new Schema(fields.ToArray());
         using ParquetWriter parquetWriter = new ParquetWriter(schema, stream);

         int idx = 0;
         while (reader.Read())
         {
            if (idx >= rowGroupSize)
            {
               WriteGroup(parquetWriter, columnInfos, idx);
               idx = 0;
            }
            for (int i = 0; i < columnInfos.Length; i++)
            {
               ColumnInfo c = columnInfos[i];
               c.writer.Process(reader, i, c.dataArray, idx);
            }
            idx++;
         }

         if (idx > 0)
         {
            WriteGroup(parquetWriter, columnInfos, idx);
         }
      }

      static void WriteGroup(ParquetWriter pw, ColumnInfo[] columns, int count)
      {
         using ParquetRowGroupWriter gw = pw.CreateRowGroup();
         for (int i = 0; i < columns.Length; i++)
         {
            ColumnInfo col = columns[i];
            Array dataArray = col.dataArray;
            // The final batch might not be a full row group so
            // we need to allocate a properly sized array and copy the data.
            // Ideally, there would be a DataColumn overload that accepted
            // an oversized array and a count, but I didn't want to change the public API
            if (count < dataArray.Length)
            {
               Array temp = col.writer.CreateDataArray(count);
               Array.Copy(dataArray, temp, count);
               dataArray = temp;
            }
            DataColumn dataColumn = new DataColumn(col.field, dataArray);
            gw.WriteColumn(dataColumn);
         }
      }
   }
}

#endif
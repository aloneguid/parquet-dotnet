using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;
using Parquet.File;

namespace Parquet.DataTypes
{

   abstract class BasicDataType<TSystemType> : IDataTypeHandler
   {
      private readonly Thrift.Type _thriftType;
      private readonly Thrift.ConvertedType? _convertedType;
      private readonly int? _bitWidth;

      public BasicDataType(DataType dataType, Thrift.Type thriftType, Thrift.ConvertedType? convertedType = null, int? bitWidth = null)
      {
         DataType = dataType;
         _thriftType = thriftType;
         _convertedType = convertedType;
         _bitWidth = bitWidth;
      }

      public int? BitWidth => _bitWidth;

      public DataType DataType { get; private set; }

      public Type ClrType => typeof(TSystemType);

      public virtual bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return
            (tse.__isset.type && _thriftType == tse.Type) &&
            (_convertedType == null || (tse.__isset.converted_type && tse.Converted_type == _convertedType.Value));
      }

      public SchemaElement CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index)
      {
         Thrift.SchemaElement tse = schema[index++];

         bool hasNulls = (tse.Repetition_type == Thrift.FieldRepetitionType.REQUIRED);
         bool isArray = (tse.Repetition_type == Thrift.FieldRepetitionType.REPEATED);

         return CreateSimple(tse, hasNulls, isArray);
      }

      protected virtual SchemaElement CreateSimple(Thrift.SchemaElement tse, bool hasNulls, bool isArray)
      {
         return new SchemaElement(tse.Name, DataType, hasNulls, isArray);
      }

      public abstract IList CreateEmptyList(Thrift.SchemaElement tse, ParquetOptions parquetOptions, int capacity);

      public abstract IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions);

      public void CreateThrift(SchemaElement se, IList<Thrift.SchemaElement> container)
      {
         var tse = new Thrift.SchemaElement(se.Name);
         tse.Type = _thriftType;
         if (_convertedType != null) tse.Converted_type = _convertedType.Value;
         tse.Repetition_type = se.IsArray
            ? Thrift.FieldRepetitionType.REPEATED
            : (se.HasNulls ? Thrift.FieldRepetitionType.OPTIONAL : Thrift.FieldRepetitionType.REQUIRED);
         container.Add(tse);
      }

      public abstract void Write(BinaryWriter writer, IList values);
   }
}

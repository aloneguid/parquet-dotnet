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

      public virtual bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return
            (tse.__isset.type && _thriftType == tse.Type) &&
            (_convertedType == null || (tse.__isset.converted_type && tse.Converted_type == _convertedType.Value));
      }

      public SchemaElement CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index)
      {
         Thrift.SchemaElement tse = schema[index++];

         if (tse.Repetition_type == Thrift.FieldRepetitionType.REPEATED)
         {
            var list = new ListSchemaElement(tse.Name);
            SchemaElement sei = CreateSimple(tse);
            list.Item = sei;
            return list;
         }
         else
         {
            SchemaElement se = CreateSimple(tse);
            return se;
         }
      }

      protected virtual SchemaElement CreateSimple(Thrift.SchemaElement tse)
      {
         return new SchemaElement(tse.Name, DataType);
      }

      public abstract IList CreateEmptyList(Thrift.SchemaElement tse, ParquetOptions parquetOptions, int capacity);

      public abstract IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions);

      public Thrift.SchemaElement CreateThriftElement(SchemaElement se, IList<Thrift.SchemaElement> container)
      {
         var tse = new Thrift.SchemaElement(se.Name);

         return tse;
      }
   }
}

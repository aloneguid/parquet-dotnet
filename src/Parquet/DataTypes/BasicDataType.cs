using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;
using Parquet.File;

namespace Parquet.DataTypes
{
   abstract class BasicPrimitiveDataType<TSystemType> : BasicDataType<TSystemType>
      where TSystemType : struct
   {
      public BasicPrimitiveDataType(Thrift.Type thriftType, Thrift.ConvertedType? convertedType = null, int? bitWidth = null) : base(thriftType, convertedType, bitWidth)
      {
      }

      public override IList CreateEmptyList(Thrift.SchemaElement tse, ParquetOptions parquetOptions, int capacity)
      {
         return tse.IsNullable()
            ? (IList)(new List<TSystemType?>())
            : (IList)(new List<TSystemType>());
      }

      public override IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, TSystemType> readOneFunc);

         var pr = new PrimitiveReader<TSystemType>(tse, formatOptions, this, reader, typeWidth, readOneFunc);

         return pr.ReadAll();
      }

      protected virtual void GetPrimitiveReaderParameters(out int typeWidth, out Func<BinaryReader, TSystemType> readOneFunc)
      {
         throw new InvalidOperationException($"'{nameof(GetPrimitiveReaderParameters)}' is not defined. Either declare it, or implement '{nameof(Read)}' yourself.");
      }
   }

   abstract class BasicDataType<TSystemType> : IDataTypeHandler
   {
      private readonly Thrift.Type _thriftType;
      private readonly Thrift.ConvertedType? _convertedType;
      private readonly int? _bitWidth;

      public BasicDataType(Thrift.Type thriftType, Thrift.ConvertedType? convertedType = null, int? bitWidth = null)
      {
         _thriftType = thriftType;
         _convertedType = convertedType;
         _bitWidth = bitWidth;
      }

      public int? BitWidth => _bitWidth;

      public virtual bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return
            (tse.__isset.type && _thriftType == tse.Type) &&
            (_convertedType == null || (tse.__isset.converted_type && tse.Converted_type == _convertedType.Value));
      }

      public SchemaElement CreateSchemaElement(SchemaElement parent, IList<Thrift.SchemaElement> schema, ref int index)
      {
         Thrift.SchemaElement tse = schema[index++];

         if(tse.Repetition_type == Thrift.FieldRepetitionType.REPEATED)
         {
            var list = new SchemaElement(tse.Name, DataType.List, parent);
            parent.NewChildren.Add(list);
            SchemaElement sei = CreateSimple(list, tse);
            list.NewChildren.Add(sei);
            return null;
         }

         SchemaElement se = CreateSimple(parent, tse);
         parent.NewChildren.Add(se);
         return null;
      }

      protected abstract SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse);

      public abstract IList CreateEmptyList(Thrift.SchemaElement tse, ParquetOptions parquetOptions, int capacity);

      public abstract IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions);
   }
}

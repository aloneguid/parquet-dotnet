using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.File;

namespace Parquet.DataTypes
{
   abstract class BasicPrimitiveDataType<TSystemType> : BasicDataType<TSystemType>
      where TSystemType : struct
   {
      public BasicPrimitiveDataType(DataType dataType, Thrift.Type thriftType, Thrift.ConvertedType? convertedType = null, int? bitWidth = null)
         : base(dataType, thriftType, convertedType, bitWidth)
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
}

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.File;

namespace Parquet.Data
{
   abstract class BasicPrimitiveDataType<TSystemType> : BasicDataType<TSystemType>
      where TSystemType : struct
   {
      public BasicPrimitiveDataType(DataType dataType, Thrift.Type thriftType, Thrift.ConvertedType? convertedType = null, int? bitWidth = null)
         : base(dataType, thriftType, convertedType, bitWidth)
      {
      }

      public override IList CreateEmptyList(bool isNullable, int capacity)
      {
         return isNullable
            ? (IList)(new List<TSystemType?>(capacity))
            : (IList)(new List<TSystemType>(capacity));
      }
   }
}

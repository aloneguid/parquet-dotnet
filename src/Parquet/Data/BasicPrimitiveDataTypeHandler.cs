using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.File;

namespace Parquet.Data
{
   abstract class BasicPrimitiveDataTypeHandler<TSystemType> : BasicDataTypeHandler<TSystemType>
      where TSystemType : struct
   {
      public BasicPrimitiveDataTypeHandler(DataType dataType, Thrift.Type thriftType, Thrift.ConvertedType? convertedType = null)
         : base(dataType, thriftType, convertedType)
      {
      }
   }
}

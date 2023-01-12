using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using Parquet.Schema;

namespace Parquet.Data {
    /// <summary>
    /// Handler for built-in data types in .NET
    /// </summary>
    abstract class BasicPrimitiveDataTypeHandler<TSystemType> : BasicDataTypeHandler<TSystemType>
      where TSystemType : struct {
        public BasicPrimitiveDataTypeHandler(DataType dataType, Thrift.Type thriftType, Thrift.ConvertedType? convertedType = null)
           : base(dataType, thriftType, convertedType) {
        }
    }
}
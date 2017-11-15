using System;
using System.Collections.Generic;
using System.Linq;

namespace Parquet.DataTypes
{
   static class DataTypeFactory
   {
      private static readonly List<IDataTypeHandler> _allDataTypes = new List<IDataTypeHandler>
      {
         // special types
         new DateTimeOffsetDataType(),
         new IntervalDataType(),
         new DecimalDataType(),

         // low priority types
         new BooleanDataType(),
         new ByteDataType(),
         new SignedByteDataType(),
         new Int16DataType(),
         new UnsignedInt16DataType(),
         new Int32DataType(),
         new Int64DataType(),
         new Int96DataType(),
         new FloatDataType(),
         new DoubleDataType(),
         new StringDataType(),
         new ByteArrayDataType(),

         // composite types
         new ListDataType(),
         new MapDataType()
      };

      public static IDataTypeHandler Match(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return _allDataTypes.FirstOrDefault(dt => dt.IsMatch(tse, formatOptions));
      }

      public static IDataTypeHandler Match(DataType dataType)
      {
         return _allDataTypes.FirstOrDefault(dt => dt.DataType == dataType);
      }
   }
}

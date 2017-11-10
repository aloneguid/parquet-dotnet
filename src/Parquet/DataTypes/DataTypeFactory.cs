using System.Collections.Generic;
using System.Linq;

namespace Parquet.DataTypes
{
   static class DataTypeFactory
   {
      private static readonly List<IDataTypeHandler> _allDataTypes = new List<IDataTypeHandler>
      {
         new BooleanDataType(),
         new Int32DataType(),
         new Int64DataType(),
         new FloatDataType(),
         new DoubleDataType(),
         new DecimalDataType(),
         new StringDataType(),
         new ByteArrayDataType(),
         new DateTimeOffsetDataType(),
         new IntervalDataType(),
         new ListDataType(),
         new MapDataType()
      };

      public static IDataTypeHandler Match(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return _allDataTypes.FirstOrDefault(dt => dt.IsMatch(tse, formatOptions));
      }
   }
}

using System.Collections.Generic;
using System.Linq;

namespace Parquet.Data {
    static class DataTypeFactory {
        private static readonly List<IDataTypeHandler> _allDataTypes = new List<IDataTypeHandler> {
         // special types
         new DateTimeOffsetDataTypeHandler(),
         new DateTimeDataTypeHandler(),
         new IntervalDataTypeHandler(),
         new DecimalDataTypeHandler(),
         new TimeSpanDataTypeHandler(),

         // low priority types
         new BooleanDataTypeHandler(),
         new ByteDataTypeHandler(), // byte is unsigned by default
         new SignedByteDataTypeHandler(),
         new Int16DataTypeHandler(),
         new UnsignedInt16DataTypeHandler(),
         new UnsignedInt32DataTypeHandler(),
         new Int32DataTypeHandler(),
         new UnsignedInt64DataTypeHandler(),
         new Int64DataTypeHandler(),
         new Int96DataTypeHandler(),
         new FloatDataTypeHandler(),
         new DoubleDataTypeHandler(),
         new StringDataTypeHandler(),
         new ByteArrayDataTypeHandler(),

         // composite types
         new ListDataTypeHandler(),
         new MapDataTypeHandler(),
         new StructureDataTypeHandler()
      };

        public static IDataTypeHandler Match(Thrift.SchemaElement tse, ParquetOptions formatOptions) {
            return _allDataTypes.FirstOrDefault(dt => dt.IsMatch(tse, formatOptions));
        }
    }
}

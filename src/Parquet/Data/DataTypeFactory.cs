using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.Schema;

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

        private static IDataTypeHandler Match(DataType dataType) {
            return _allDataTypes.FirstOrDefault(dt => dt.DataType == dataType);
        }

        public static IDataTypeHandler Match(Field field) {
            switch(field.SchemaType) {
                case SchemaType.Struct:
                    return new StructureDataTypeHandler();
                case SchemaType.Map:
                    return new MapDataTypeHandler();
                case SchemaType.List:
                    return new ListDataTypeHandler();
                case SchemaType.Data:
                    return Match(((DataField)field).DataType);
                default:
                    throw OtherExtensions.NotImplemented($"matching {field.SchemaType}");
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.Data.Concrete;

namespace Parquet.Data
{
   static class DataTypeFactory
   {
      private static readonly List<IDataTypeHandler> _allDataTypes = new List<IDataTypeHandler>
      {
         // special types
         new DateTimeOffsetDataTypeHandler(),
         new DateTimeDataTypeHandler(),
         new IntervalDataTypeHandler(),
         new DecimalDataTypeHandler(),

         // low priority types
         new BooleanDataTypeHandler(),
         new ByteDataTypeHandler(),
         new SignedByteDataTypeHandler(),
         new Int16DataTypeHandler(),
         new UnsignedInt16DataTypeHandler(),
         new Int32DataTypeHandler(),
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

      //todo: all the matches can be much faster, cache them.

      public static IDataTypeHandler Match(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return _allDataTypes.FirstOrDefault(dt => dt.IsMatch(tse, formatOptions));
      }

      public static IDataTypeHandler Match(DataType dataType)
      {
         return _allDataTypes.FirstOrDefault(dt => dt.DataType == dataType);
      }

      public static IDataTypeHandler Match(Field field)
      {
         switch(field.SchemaType)
         {
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

      public static IDataTypeHandler Match(Type clrType)
      {
         return _allDataTypes.FirstOrDefault(dt => dt.ClrType == clrType);
      }

      public static void ThrowClrTypeNotSupported(Type clrType)
      {
         string message = string.Format("CLR type '{0}' is not supported, please specify one of '{1}' or use an alternative constructor",
            clrType,
            string.Join(", ", _allDataTypes.Select(dt => dt.ClrType))
            );

         throw new NotSupportedException(message);
      }
   }
}

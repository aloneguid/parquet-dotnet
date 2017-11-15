using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class MapDataType : IDataTypeHandler
   {
      public MapDataType()
      {
      }

      public int? BitWidth => null;

      public DataType DataType => DataType.Dictionary;

      public IList CreateEmptyList(Thrift.SchemaElement tse, ParquetOptions parquetOptions, int capacity)
      {
         throw new NotImplementedException();
      }

      public SchemaElement CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index)
      {
         Thrift.SchemaElement tseRoot = schema[index];

         //next element is a container
         Thrift.SchemaElement tseContainer = schema[++index];

         //followed by a key and a value
         Thrift.SchemaElement tseKey = schema[++index];
         Thrift.SchemaElement tseValue = schema[++index];

         var map = new MapSchemaElement(tseRoot.Name);

         //go to next
         index += 1;

         return map;
      }

      public bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return
            tse.__isset.converted_type &&
            (tse.Converted_type == Thrift.ConvertedType.MAP || tse.Converted_type == Thrift.ConvertedType.MAP_KEY_VALUE);
      }

      public IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         throw new NotImplementedException();
      }
   }
}

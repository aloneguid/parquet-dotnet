using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.DataTypes.Concrete
{
   class StructureDataType : IDataTypeHandler
   {
      public int? BitWidth => null;

      public DataType DataType => DataType.Structure;

      public IList CreateEmptyList(Thrift.SchemaElement tse, ParquetOptions parquetOptions, int capacity)
      {
         throw new NotSupportedException("structures cannot have row values");
      }

      public SchemaElement CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index)
      {
         Thrift.SchemaElement container = schema[index];

         index += 2; //this element plus inline wasted container

         return new StructureSchemaElement(container.Name);
      }

      public bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return
            tse.Num_children > 0;
      }

      public IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         throw new NotSupportedException("structures are never to be read individually");
      }
   }
}

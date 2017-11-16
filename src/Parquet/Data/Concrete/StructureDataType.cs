using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.Data.Concrete
{
   class StructureDataType : IDataTypeHandler
   {
      public int? BitWidth => null;

      public DataType DataType => DataType.Structure;

      public Type ClrType => typeof(Row);

      System.Type IDataTypeHandler.ClrType => throw new NotImplementedException();

      public IList CreateEmptyList(bool isNullable, int capacity)
      {
         throw new NotSupportedException("structures cannot have row values");
      }

      public SchemaElement CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index)
      {
         Thrift.SchemaElement container = schema[index];

         index += 2; //this element plus inline wasted container

         return new StructureSchemaElement(container.Name, false);
      }

      public void CreateThrift(Data.SchemaElement se, IList<Thrift.SchemaElement> container)
      {
         throw new NotImplementedException();
      }

      public void CreateThrift(SchemaElement se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container)
      {
         throw new NotImplementedException();
      }

      public Thrift.SchemaElement CreateThriftElement(Data.SchemaElement se, IList<Thrift.SchemaElement> container)
      {
         throw new NotImplementedException();
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

      public void Write(BinaryWriter writer, IList values)
      {
         throw new NotImplementedException();
      }
   }
}

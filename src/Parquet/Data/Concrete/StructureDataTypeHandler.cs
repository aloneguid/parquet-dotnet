using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.Data.Concrete
{
   class StructureDataTypeHandler : IDataTypeHandler
   {
      public DataType DataType => DataType.Unspecified;

      public SchemaType SchemaType => SchemaType.Structure;

      public Type ClrType => typeof(Row);

      Type IDataTypeHandler.ClrType => throw new NotImplementedException();

      public IList CreateEmptyList(bool isNullable, bool isArray, int capacity)
      {
         throw new NotSupportedException("structures cannot have row values");
      }

      public Field CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index, out int ownedChildCount)
      {
         Thrift.SchemaElement container = schema[index];

         index += 2; //this element plus inline wasted container

         throw new NotImplementedException();
         ownedChildCount = 0;
         return new StructureSchemaElement(container.Name, false);
      }

      public void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container)
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

      public void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         throw new NotImplementedException();
      }
   }
}

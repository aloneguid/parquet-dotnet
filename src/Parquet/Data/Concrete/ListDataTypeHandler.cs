using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.Data.Concrete
{
   class ListDataTypeHandler : IDataTypeHandler
   {
      public ListDataTypeHandler()
      {
      }

      public DataType DataType => DataType.Unspecified;

      public SchemaType SchemaType => SchemaType.Structure;

      public Type ClrType => null;

      public IList CreateEmptyList(bool isNullable, bool isArray, int capacity)
      {
         throw new NotImplementedException();
      }

      public Field CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index, out int ownedChildCount)
      {
         Thrift.SchemaElement tseList = schema[index];

         ListField listField = ListField.CreateWithNoItem(tseList.Name);
         //as we are skipping elements set path hint
         listField.Path = $"{tseList.Name}{Schema.PathSeparator}{schema[index + 1].Name}";
         index += 2;          //skip this element and child container
         ownedChildCount = 1; //we should get this element assigned back
         return listField;
      }

      public void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container)
      {
         throw new NotImplementedException();
      }

      public bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.LIST;
      }

      public IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         throw new NotImplementedException();
      }

      public void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         throw new NotImplementedException();
      }
   }
}

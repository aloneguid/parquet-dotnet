using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class ListDataType : IDataTypeHandler
   {
      public ListDataType()
      {
      }

      public int? BitWidth => null;

      public DataType DataType => DataType.List;

      public Type ClrType => typeof(IEnumerable<>);

      public IList CreateEmptyList(Thrift.SchemaElement tse, ParquetOptions parquetOptions, int capacity)
      {
         return new List<IEnumerable>(capacity);
      }

      public SchemaElement CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index)
      {
         var list = new ListSchemaElement(schema[index].Name);

         //skip this element and child container
         index += 2;

         //parent parser should add more elements inside this structure

         return list;
      }

      public void CreateThrift(Data.SchemaElement se, IList<Thrift.SchemaElement> container)
      {
         throw new NotImplementedException();
      }

      public Thrift.SchemaElement CreateThriftElement(SchemaElement se, IList<Thrift.SchemaElement> container)
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

      public void Write(BinaryWriter writer, IList values)
      {
         throw new NotImplementedException();
      }
   }
}

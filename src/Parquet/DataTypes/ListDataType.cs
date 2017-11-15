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

      public SchemaElement CreateSchemaElement(SchemaElement parent, IList<Thrift.SchemaElement> schema, ref int index)
      {
         var list = new SchemaElement(schema[index].Name, DataType.List, parent);
         parent.NewChildren.Add(list);

         //skip this element and child container
         index += 2;

         return list;
      }

      public bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return tse.__isset.converted_type && tse.Converted_type == Thrift.ConvertedType.LIST;
      }

      public IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         throw new NotImplementedException();
      }
   }
}

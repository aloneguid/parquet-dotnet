using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class ListDataType : IDataType
   {
      public int? BitWidth => null;

      public SchemaElement Create(SchemaElement parent, IList<Thrift.SchemaElement> schema, ref int index)
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

      public IList Read(byte[] data)
      {
         throw new NotImplementedException();
      }
   }
}

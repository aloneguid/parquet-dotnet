using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Parquet.Thrift;

namespace Parquet.DataTypes
{
   class ListDataType : IDataType
   {
      public int? BitWidth => null;

      public SchemaElement2 Create(SchemaElement2 parent, IList<Thrift.SchemaElement> schema, ref int index)
      {
         var list = new SchemaElement2(schema[index].Name, DataType.List, parent);
         parent.Children.Add(list);

         //skip this element and child container
         index += 2;

         return list;
      }

      public bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return tse.__isset.converted_type && tse.Converted_type == ConvertedType.LIST;
      }
   }
}

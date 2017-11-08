using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Parquet.Thrift;

namespace Parquet.DataTypes
{
   class MapDataType : IDataType
   {
      public int? BitWidth => null;

      public SchemaElement2 Create(SchemaElement2 parent, IList<Thrift.SchemaElement> schema, ref int index)
      {
         //next element is a container
         Thrift.SchemaElement tseContainer = schema[++index];

         //followed by a key and a value
         Thrift.SchemaElement tseKey = schema[++index];
         Thrift.SchemaElement tseValue = schema[++index];

         var map = new SchemaElement2(tseContainer.Name, DataType.Dictionary, parent);
         parent.Children.Add(map);

         //go to next
         index += 1;

         return null;   //no children here
      }

      public bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return
            tse.__isset.converted_type &&
            (tse.Converted_type == ConvertedType.MAP || tse.Converted_type == ConvertedType.MAP_KEY_VALUE);
      }
   }
}

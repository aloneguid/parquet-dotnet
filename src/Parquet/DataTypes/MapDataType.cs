using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;

namespace Parquet.DataTypes
{
   class MapDataType : IDataType
   {
      public int? BitWidth => null;

      public SchemaElement Create(SchemaElement parent, IList<Thrift.SchemaElement> schema, ref int index)
      {
         //next element is a container
         Thrift.SchemaElement tseContainer = schema[++index];

         //followed by a key and a value
         Thrift.SchemaElement tseKey = schema[++index];
         Thrift.SchemaElement tseValue = schema[++index];

         var map = new SchemaElement(tseContainer.Name, DataType.Dictionary, parent);
         parent.NewChildren.Add(map);

         //go to next
         index += 1;

         return null;   //no children here
      }

      public bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return
            tse.__isset.converted_type &&
            (tse.Converted_type == Thrift.ConvertedType.MAP || tse.Converted_type == Thrift.ConvertedType.MAP_KEY_VALUE);
      }

      public IList Read(byte[] data)
      {
         throw new NotImplementedException();
      }
   }
}

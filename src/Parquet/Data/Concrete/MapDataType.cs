using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Parquet.Data;

namespace Parquet.Data
{
   class MapDataType : IDataTypeHandler
   {
      public MapDataType()
      {
      }

      public int? BitWidth => null;

      public DataType DataType => DataType.Dictionary;

      public Type ClrType => typeof(IDictionary<,>);

      public IList CreateEmptyList(bool isNullable, int capacity)
      {
         throw new NotImplementedException();
      }

      public SchemaElement CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index, out int ownedChildCount)
      {
         Thrift.SchemaElement tseRoot = schema[index];

         //next element is a container
         Thrift.SchemaElement tseContainer = schema[++index];

         if(tseContainer.Num_children != 2)
         {
            throw new IndexOutOfRangeException($"dictionary container must have exactly 2 children but {tseContainer.Num_children} found");
         }

         //followed by a key and a value, but we declared them as owned

         var map = new DictionarySchemaElement(tseRoot.Name);
         map.Path = tseRoot.Name + Schema.PathSeparator + tseContainer.Name;
         index += 1;
         ownedChildCount = 2;
         return map;
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
            tse.__isset.converted_type &&
            (tse.Converted_type == Thrift.ConvertedType.MAP || tse.Converted_type == Thrift.ConvertedType.MAP_KEY_VALUE);
      }

      public IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         throw new NotImplementedException();
      }

      public void Write(BinaryWriter writer, IList values)
      {
         throw new NotImplementedException();
      }

      public void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         throw new NotImplementedException();
      }
   }
}

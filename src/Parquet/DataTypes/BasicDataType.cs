using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;

namespace Parquet.DataTypes
{
   abstract class BasicDataType<TSystemType> : IDataTypeHandler
   {
      private readonly Thrift.Type _thriftType;
      private readonly Thrift.ConvertedType? _convertedType;
      private readonly int? _bitWidth;

      public BasicDataType(Thrift.Type thriftType, Thrift.ConvertedType? convertedType = null, int? bitWidth = null)
      {
         _thriftType = thriftType;
         _convertedType = convertedType;
         _bitWidth = bitWidth;
      }

      public int? BitWidth => _bitWidth;

      public virtual bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions)
      {
         return tse.__isset.type && _thriftType == tse.Type;
      }

      public SchemaElement Create(SchemaElement parent, IList<Thrift.SchemaElement> schema, ref int index)
      {
         Thrift.SchemaElement tse = schema[index++];

         if(tse.Repetition_type == Thrift.FieldRepetitionType.REPEATED)
         {
            var list = new SchemaElement(tse.Name, DataType.List, parent);
            parent.NewChildren.Add(list);
            SchemaElement sei = CreateSimple(list, tse);
            list.NewChildren.Add(sei);
            return null;
         }

         SchemaElement se = CreateSimple(parent, tse);
         parent.NewChildren.Add(se);
         return null;
      }

      protected abstract SchemaElement CreateSimple(SchemaElement parent, Thrift.SchemaElement tse);

      public abstract IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions);
   }
}

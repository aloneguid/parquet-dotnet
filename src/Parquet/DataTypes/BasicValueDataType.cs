using System;
using System.Collections;
using System.Collections.Generic;

namespace Parquet.DataTypes
{
   abstract class BasicValueDataType<TSystemType> : IDataType
      where TSystemType : struct
   {
      private readonly Thrift.Type _thriftType;
      private readonly Thrift.ConvertedType? _convertedType;
      private readonly int? _bitWidth;

      public BasicValueDataType(Thrift.Type thriftType, Thrift.ConvertedType? convertedType = null, int? bitWidth = null)
      {
         _thriftType = thriftType;
         _convertedType = convertedType;
         _bitWidth = bitWidth;
      }

      public int? BitWidth => _bitWidth;

      public void Build(Data.SchemaElement element, IList<Thrift.SchemaElement> schema)
      {
         throw new NotImplementedException();
      }

      public IList CreateList(bool nullable, int capacity)
      {
         return nullable
            ? (IList)new List<TSystemType?>(capacity)
            : (IList)new List<TSystemType>(capacity);
      }

      public bool IsMatch(Thrift.SchemaElement tse)
      {
         return tse.__isset.type && _thriftType == tse.Type;
      }

      public Data.SchemaElement Parse(Data.SchemaElement root, ICollection<Thrift.SchemaElement> schema, ref int index)
      {
         throw new NotImplementedException();
      }
   }
}

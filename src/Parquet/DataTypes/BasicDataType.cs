using System.Collections.Generic;
using Parquet.Data;

namespace Parquet.DataTypes
{
   abstract class BasicDataType<TSystemType> : IDataType
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

      public SchemaElement2 Create(SchemaElement2 parent, IList<Thrift.SchemaElement> schema, ref int index)
      {
         Thrift.SchemaElement tse = schema[index++];

         if(tse.Repetition_type == Thrift.FieldRepetitionType.REPEATED)
         {
            var list = new SchemaElement2(tse.Name, DataType.List, parent);
            parent.Children.Add(list);
            SchemaElement2 sei = CreateSimple(list, tse);
            list.Children.Add(sei);
            return null;
         }

         SchemaElement2 se = CreateSimple(parent, tse);
         parent.Children.Add(se);
         return null;
      }

      protected abstract SchemaElement2 CreateSimple(SchemaElement2 parent, Thrift.SchemaElement tse);
   }
}

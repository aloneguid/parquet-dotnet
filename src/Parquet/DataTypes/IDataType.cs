using System.Collections;
using System.Collections.Generic;
using Parquet.Data;

namespace Parquet.DataTypes
{
   /// <summary>
   /// Prototype: data type interface
   /// </summary>
   interface IDataType
   {
      bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions);

      SchemaElement2 Create(SchemaElement2 parent, IList<Thrift.SchemaElement> schema, ref int index);

      int? BitWidth { get; }
   }
}
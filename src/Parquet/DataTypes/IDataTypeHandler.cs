using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;

namespace Parquet.DataTypes
{
   /// <summary>
   /// Prototype: data type interface
   /// </summary>
   interface IDataTypeHandler
   {
      bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions);

      SchemaElement CreateSchemaElement(SchemaElement parent, IList<Thrift.SchemaElement> schema, ref int index);

      int? BitWidth { get; }

      IList CreateEmptyList(Thrift.SchemaElement tse, ParquetOptions parquetOptions, int capacity);

      IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions);
   }
}
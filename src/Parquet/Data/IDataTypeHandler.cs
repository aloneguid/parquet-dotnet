using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Parquet.Data;

namespace Parquet.Data
{
   /// <summary>
   /// Prototype: data type interface
   /// </summary>
   interface IDataTypeHandler
   {
      bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions);

      Field CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index, out int ownedChildCount);

      void CreateThrift(Field se, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container);

      DataType DataType { get; }

      SchemaType SchemaType { get; }

      Type ClrType { get; }

      IList CreateEmptyList(bool isNullable, bool isArray, int capacity);

      IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions);

      void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values);

   }
}
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Parquet.Data
{
   abstract class NonDataDataTypeHandler : IDataTypeHandler
   {
      public NonDataDataTypeHandler()
      {
      }

      public DataType DataType => DataType.Unspecified;

      public abstract SchemaType SchemaType { get; }

      public Type ClrType => null;

      public abstract Field CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index, out int ownedChildCount);

      public abstract void CreateThrift(Field field, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container);

      public abstract bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions);
      public Array GetArray(int minCount, bool rent, bool isNullable)
      {
         throw new NotSupportedException();
      }

      public IList Read(Thrift.SchemaElement tse, BinaryReader reader, ParquetOptions formatOptions)
      {
         throw new NotSupportedException();
      }

      public int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset, ParquetOptions formatOptions)
      {
         throw new NotSupportedException();
      }

      public void ReturnArray(Array array, bool isNullable)
      {
         throw new NotSupportedException();
      }

      public void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values)
      {
         throw new NotSupportedException();
      }

      public Array MergeDictionary(Array dictionary, int[] indexes)
      {
         throw new NotSupportedException();
      }

      public Array UnpackDefinitions(Array src, int[] definitionLevels, int maxDefinitionLevel)
      {
         throw new NotSupportedException();
      }

      public TypedArrayWrapper CreateTypedArrayWrapper(Array array, bool isNullable)
      {
         throw new NotSupportedException();
      }
   }
}

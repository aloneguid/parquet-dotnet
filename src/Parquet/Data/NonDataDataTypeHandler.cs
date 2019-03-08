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

      public int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset) => throw new NotSupportedException();

      public object Read(BinaryReader reader, Thrift.SchemaElement tse, int length) => throw new NotSupportedException();


      public void Write(Thrift.SchemaElement tse, BinaryWriter writer, IList values, Thrift.Statistics statistics)
      {
         throw new NotSupportedException();
      }

      public Array MergeDictionary(Array dictionary, int[] indexes, Array data, int offset, int length)
      {
         throw new NotSupportedException();
      }

      public Array PackDefinitions(Array data,  int maxDefiniionLevel, out int[] definitions, out int definitionsLength, out int nullCount)
      {
         throw new NotImplementedException();
      }

      public Array UnpackDefinitions(Array src, int[] definitionLevels, int maxDefinitionLevel, out bool[] hasValueFlags)
      {
         throw new NotSupportedException();
      }

   }
}

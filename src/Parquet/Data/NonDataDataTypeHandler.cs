using System;
using System.Collections.Generic;
using System.IO;
using Parquet.Schema;

namespace Parquet.Data {
    abstract class NonDataDataTypeHandler : IDataTypeHandler {
        public NonDataDataTypeHandler() {
        }

        public DataType DataType => DataType.Unspecified;

        public abstract SchemaType SchemaType { get; }

        public System.Type ClrType => null;

        public abstract Field CreateSchemaElement(IList<Thrift.SchemaElement> schema, ref int index, out int ownedChildCount);

        public abstract void CreateThrift(Field field, Thrift.SchemaElement parent, IList<Thrift.SchemaElement> container);

        public abstract bool IsMatch(Thrift.SchemaElement tse, ParquetOptions formatOptions);

        public int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset) => throw new NotSupportedException();

        public object Read(BinaryReader reader, Thrift.SchemaElement tse, int length) => throw new NotSupportedException();

        public Array MergeDictionary(Array dictionary, int[] indexes, Array data, int offset, int length) {
            throw new NotSupportedException();
        }

        public ArrayView PackDefinitions(Array data, int offset, int count, int maxDefiniionLevel, out int[] definitions, out int definitionsLength, out int nullCount) {
            throw new NotImplementedException();
        }

        public object PlainDecode(Thrift.SchemaElement tse, byte[] encoded) => throw new NotImplementedException();
    }
}
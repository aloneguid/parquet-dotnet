using System;
using System.IO;
using Parquet.Schema;
using Parquet.Thrift;

namespace Parquet.Data.Concrete {
    class Int32DataTypeHandler : BasicPrimitiveDataTypeHandler<int> {
        public Int32DataTypeHandler() : base(DataType.Int32, Thrift.Type.INT32) {
        }

        protected override int ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length) {
            return reader.ReadInt32();
        }

        public override int Read(BinaryReader reader, SchemaElement tse, Array dest, int offset) {
            Span<int> span = ((int[])dest).AsSpan(offset);
            return ParquetEncoder.Decode(reader.BaseStream, span);
        }
    }
}
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

        public override void Write(SchemaElement tse, BinaryWriter writer, ArrayView values, DataColumnStatistics statistics) {

            Span<int> span = ((int[])values.Data).AsSpan(values.Offset, values.Count);  // temporary

            ParquetEncoder.Encode(span, writer.BaseStream);
            ParquetEncoder.FillStats(span, statistics);
        }

        protected override void WriteOne(BinaryWriter writer, int value) {
            writer.Write(value);
        }
    }
}
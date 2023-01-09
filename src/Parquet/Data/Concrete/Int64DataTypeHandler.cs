using System;
using System.Collections;
using System.IO;
using Parquet.Schema;

namespace Parquet.Data.Concrete {
    class Int64DataTypeHandler : BasicPrimitiveDataTypeHandler<long> {
        public Int64DataTypeHandler() : base(DataType.Int64, Thrift.Type.INT64) {
        }

        protected override long ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length) {
            return reader.ReadInt64();
        }
    }
}

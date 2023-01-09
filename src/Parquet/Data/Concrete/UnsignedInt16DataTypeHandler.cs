using System;
using System.IO;
using Parquet.Schema;

namespace Parquet.Data.Concrete {
    class UnsignedInt16DataTypeHandler : BasicPrimitiveDataTypeHandler<UInt16> {
        public UnsignedInt16DataTypeHandler() : base(DataType.UnsignedInt16, Thrift.Type.INT32, Thrift.ConvertedType.UINT_16) {

        }

        protected override ushort ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length) {
            return (ushort)reader.ReadUInt32();
        }

    }
}

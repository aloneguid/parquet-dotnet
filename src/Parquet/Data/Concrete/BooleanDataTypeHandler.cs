using System;
using System.Collections;
using System.IO;
using Parquet.Schema;
using Parquet.Thrift;

namespace Parquet.Data.Concrete {
    class BooleanDataTypeHandler : BasicPrimitiveDataTypeHandler<bool> {
        public BooleanDataTypeHandler() : base(DataType.Boolean, Thrift.Type.BOOLEAN) {
        }

        public override int Read(BinaryReader reader, Thrift.SchemaElement tse, Array dest, int offset) {
            int start = offset;

            int ibit = 0;
            bool[] bdest = (bool[])dest;

            while(reader.BaseStream.Position < reader.BaseStream.Length && offset < dest.Length) {
                byte b = reader.ReadByte();

                while(ibit < 8 && offset < dest.Length) {
                    bool set = ((b >> ibit++) & 1) == 1;
                    bdest[offset++] = set;
                }

                ibit = 0;
            }


            return offset - start;
        }

        /// <summary>
        /// Normally bools are packed, which is implemented in <see cref="Read(BinaryReader, Thrift.SchemaElement, Array, int)"/>
        /// </summary>
        protected override bool ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length) {
            return reader.ReadBoolean();
        }

        public override object PlainDecode(SchemaElement tse, byte[] encoded) => null;
    }
}
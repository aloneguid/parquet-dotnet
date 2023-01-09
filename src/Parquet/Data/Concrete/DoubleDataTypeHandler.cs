﻿using System;
using System.IO;
using Parquet.Schema;

namespace Parquet.Data.Concrete {
    class DoubleDataTypeHandler : BasicPrimitiveDataTypeHandler<double> {
        public DoubleDataTypeHandler() : base(DataType.Double, Thrift.Type.DOUBLE) {

        }

        protected override double ReadSingle(BinaryReader reader, Thrift.SchemaElement tse, int length) {
            return reader.ReadDouble();
        }
    }
}

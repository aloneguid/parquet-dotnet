using System;
using System.IO;
using Parquet.Schema;

// MIGRATION LEFTOVERS

namespace Parquet.Data.Concrete {

    class BooleanDataTypeHandler : BasicPrimitiveDataTypeHandler<bool> {
        public BooleanDataTypeHandler() : base(DataType.Boolean, Thrift.Type.BOOLEAN) {
        }
    }

    class Int32DataTypeHandler : BasicPrimitiveDataTypeHandler<int> {
        public Int32DataTypeHandler() : base(DataType.Int32, Thrift.Type.INT32) {
        }
    }

    class Int64DataTypeHandler : BasicPrimitiveDataTypeHandler<long> {
        public Int64DataTypeHandler() : base(DataType.Int64, Thrift.Type.INT64) {
        }
    }

    class DoubleDataTypeHandler : BasicPrimitiveDataTypeHandler<double> {
        public DoubleDataTypeHandler() : base(DataType.Double, Thrift.Type.DOUBLE) {

        }
    }

    class FloatDataTypeHandler : BasicPrimitiveDataTypeHandler<float> {
        public FloatDataTypeHandler() : base(DataType.Float, Thrift.Type.FLOAT) {
        }
    }
}

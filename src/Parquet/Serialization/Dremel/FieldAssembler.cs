using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Serialization.Dremel {
    class FieldAssembler<TClass> : FieldWorker<TClass> {

        public FieldAssembler(ParquetSchema schema, DataField field, Action<IEnumerable<TClass>, DataColumn> assembler, Expression expression, Expression iterationExpression) 
            : base(schema, field, expression, iterationExpression) {
            Assemble = assembler;
        }

        public Action<IEnumerable<TClass>, DataColumn> Assemble { get; }
    }
}

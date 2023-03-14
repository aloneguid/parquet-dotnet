using System.Collections.Generic;
using System.Linq;
using Parquet.Schema;

namespace Parquet.Serialization.Dremel {
    class Assembler<TClass> {
        public Assembler(ParquetSchema schema) {
            Schema = schema;

            FieldAssemblers = schema
                .GetDataFields()
                .Select(df => new FieldAssemblerCompiler<TClass>(schema, df).Compile())
                .ToList();
        }

        public ParquetSchema Schema { get; }

        public List<FieldAssembler<TClass>> FieldAssemblers { get; }
    }
}

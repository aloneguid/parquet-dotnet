using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.Schema;

namespace Parquet.Serialization.Dremel {
    class Assembler<TClass> {
        public Assembler(ParquetSchema schema) {
            Schema = schema;

            FieldAssemblers = schema
                .GetDataFields()
                .Select(df => Compile(schema, df))
                .ToList();
        }

        private static FieldAssembler<TClass> Compile(ParquetSchema schema, DataField df) {
            try {
                return new FieldAssemblerCompiler<TClass>(schema, df).Compile();
            } catch(Exception ex) {
                throw new FieldAccessException($"failed to compile '{df}'", ex);
            }
        }

        public ParquetSchema Schema { get; }

        public List<FieldAssembler<TClass>> FieldAssemblers { get; }
    }
}

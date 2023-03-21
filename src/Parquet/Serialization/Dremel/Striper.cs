using System.Collections.Generic;
using System.Linq;
using Parquet.Schema;

namespace Parquet.Serialization.Dremel {
    /// <summary>
    /// Not a stripper
    /// </summary>
    class Striper<TClass> {

        public Striper(ParquetSchema schema) {
            Schema = schema;

            FieldStripers = schema
                .GetDataFields()
                .Select(CreateStriper)
                .ToList();
        }

        public ParquetSchema Schema { get; }

        public IReadOnlyList<FieldStriper<TClass>> FieldStripers { get; }

        private FieldStriper<TClass> CreateStriper(DataField df) {
            return new FieldStriperCompiler<TClass>(Schema, df).Compile();
        }

        public static Striper<TClass> Create() {
            return new Striper<TClass>(typeof(TClass).GetParquetSchema(false));
        }
    }
}

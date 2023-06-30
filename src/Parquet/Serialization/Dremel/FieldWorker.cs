using System.Linq.Expressions;
using Parquet.Schema;

namespace Parquet.Serialization.Dremel {
    abstract class FieldWorker<TClass> {
        public ParquetSchema Schema { get; }

        public DataField Field { get; }

        public Expression Expression { get; }

        public Expression IterationExpression { get; }

        protected FieldWorker(ParquetSchema schema, DataField field, Expression expression, Expression iterationExpression) {
            Schema = schema;
            Field = field;
            Expression = expression;
            IterationExpression = iterationExpression;
        }

        public override string ToString() => Field.ToString();
    }
}

using System.Linq.Expressions;
using Parquet.Schema;

namespace Parquet.Serialization.Dremel {
    class FieldWorker<TClass> {
        public DataField Field { get; }

        public Expression Expression { get; }

        public Expression IterationExpression { get; }

        public FieldWorker(DataField field, Expression expression, Expression iterationExpression) {
            Field = field;
            Expression = expression;
            IterationExpression = iterationExpression;
        }

        public override string ToString() => Field.ToString();
    }
}

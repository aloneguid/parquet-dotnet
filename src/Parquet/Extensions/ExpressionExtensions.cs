using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Parquet.Extensions {
    static class ExpressionExtensions {
        public static Expression Loop(this Expression iteration,
            Expression collection,
            Type elementType,
            ParameterExpression element) {

            Type enumeratorGenericType = typeof(IEnumerator<>).MakeGenericType(elementType);
            Type enumerableGenericType = typeof(IEnumerable<>).MakeGenericType(elementType);

            ParameterExpression enumeratorVar = Expression.Variable(enumeratorGenericType, "enumerator");
            MethodCallExpression getEnumeratorCall = Expression.Call(collection,
                enumerableGenericType.GetMethod(nameof(IEnumerable.GetEnumerator))!);
            MethodCallExpression moveNextCall = Expression.Call(enumeratorVar,
                typeof(IEnumerator).GetMethod(nameof(IEnumerator.MoveNext))!);
            LabelTarget loopBreakLabel = Expression.Label("loopBreak");

            // doc: Expression.Loop is an infinite loop that can be exited with "break"
            LoopExpression loop = Expression.Loop(
                Expression.IfThenElse(

                    // test
                    Expression.Equal(moveNextCall, Expression.Constant(true)),

                    // if true
                    Expression.Block(
                        //new[] { classElementVar },

                        // get class element into loopVar
                        Expression.Assign(element, Expression.Property(enumeratorVar, nameof(IEnumerator.Current))),

                        iteration),

                    // if false
                    Expression.Break(loopBreakLabel)
                    ), loopBreakLabel);

            return Expression.Block(
                new[] { enumeratorVar, element },

                Expression.IfThen(Expression.NotEqual(collection, Expression.Constant(null)),
                    Expression.Block(
                        // get enumerator from class collection
                        Expression.Assign(enumeratorVar, getEnumeratorCall),

                        // loop over classes
                        loop)
                ));
        }
    }
}

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Parquet.Extensions {
    static class ExpressionExtensions {
        public static Expression Loop(this Expression iteration,
            Expression collection,
            Type elementType,
            ParameterExpression element,
            ParameterExpression? countVar = null) {

            Type enumeratorGenericType = typeof(IEnumerator<>).MakeGenericType(elementType);
            Type enumerableGenericType = typeof(IEnumerable<>).MakeGenericType(elementType);

            ParameterExpression enumeratorVar = Expression.Variable(enumeratorGenericType);
            MethodCallExpression getEnumeratorCall = Expression.Call(collection,
                enumerableGenericType.GetMethod(nameof(IEnumerable.GetEnumerator))!);
            MethodCallExpression moveNextCall = Expression.Call(enumeratorVar,
                typeof(IEnumerator).GetMethod(nameof(IEnumerator.MoveNext))!);
            LabelTarget loopBreakLabel = Expression.Label();

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

                        iteration,
                        
                        countVar == null
                            ? Expression.Empty()
                            : Expression.PostIncrementAssign(countVar)),

                    // if false
                    Expression.Break(loopBreakLabel)
                    ), loopBreakLabel);

            return Expression.Block(
                new[] { enumeratorVar, element },

                // get enumerator from class collection
                Expression.Assign(enumeratorVar, getEnumeratorCall),

                // loop over classes
                loop);
        }

        public static Expression ForLoop(this Expression fromVar, Expression toVar,
            ParameterExpression iVar, Expression body) {

            LabelTarget loopBreakLabel = Expression.Label();
            return Expression.Block(
                new[] { iVar },
                Expression.Assign(iVar, fromVar),

                Expression.Loop(
                    Expression.IfThenElse(
                        Expression.LessThan(iVar, toVar),
                        
                        Expression.Block(
                            body,
                            Expression.PostIncrementAssign(iVar)),
                        
                        Expression.Break(loopBreakLabel)),
                    loopBreakLabel)
                );
        }

        /// <summary>
        /// Calls <see cref="Array.Clear(Array, int, int)"/>
        /// </summary>
        public static Expression ClearArray(this ParameterExpression array, Expression? fromIndexVar = null) {

            Expression from = fromIndexVar ?? Expression.Constant(0);
            Expression length = Expression.Property(array, nameof(Array.Length));
            if(fromIndexVar != null) length = Expression.Subtract(length, fromIndexVar);

            return Expression.Call(
                typeof(Array).GetMethod(
                    nameof(Array.Clear),
                    BindingFlags.Static | BindingFlags.Public,
                    null,
                    new[] { typeof(Array), typeof(int), typeof(int) },
                    null)!,
                array, from, length);
        }

        public static Expression CollectionCount(this Expression collection, Type collectionType) {
            return Expression.Property(collection, nameof(IReadOnlyCollection<int>.Count));
        }

        public static Expression CollectionAdd(this Expression collection, Type collectionType, Expression element, Type elementType) {

            MethodInfo? method = collectionType.GetMethod(nameof(IList.Add), new[] { elementType });

            if(method == null)
                throw new NotSupportedException($"can't find {nameof(IList.Add)} method");

            return Expression.Call(
                collection,
                method,
                element);
        }

        public static Expression IsNull(this Expression nullableVar) {
            return Expression.Equal(nullableVar, Expression.Constant(null));
        }

        /// <summary>
        /// Gets internal property "DebugView" which is normally only available in Visual Studio debugging session
        /// </summary>
        /// <returns></returns>
        public static string GetPseudoCode(this Expression expression) {
            PropertyInfo? propertyInfo = typeof(Expression).GetProperty("DebugView", BindingFlags.Instance | BindingFlags.NonPublic);
            if(propertyInfo == null)
                return string.Empty;

            return (string)propertyInfo.GetValue(expression)!;
        }
    }
}

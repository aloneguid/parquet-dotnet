using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Parquet.Serialization {

    internal static class PreConstructor<T> {
        public static readonly Func<T> AllocateNew = Expression.Lambda<Func<T>>(
            PreConstructor.AllocateNew(typeof(T))
        ).Compile();
    }

    internal static class PreConstructor {
        public static Expression AllocateNew(Type type) {
            ConstructorInfo? defaultConstructor = type.GetConstructor(Type.EmptyTypes);
            if (defaultConstructor != null) {
                return Expression.New(type);
            }

#if NET6_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
            MethodInfo getUninit = typeof(RuntimeHelpers).GetMethod(nameof(RuntimeHelpers.GetUninitializedObject))!;
#else
            MethodInfo getUninit = typeof(FormatterServices).GetMethod(nameof(FormatterServices.GetUninitializedObject))!;
#endif
            return Expression.Convert(Expression.Call(getUninit, Expression.Constant(type)), type);
        }
    }
}

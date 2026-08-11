using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Parquet.Extensions;

namespace Parquet.Serialization.Dremel;

using ROMC = ReadOnlyMemory<char>;
using ROMB = ReadOnlyMemory<byte>;

static class DataConverter {
    #region [ Conversion adapters ]

    private static string ConvertRomOfCharToString(ReadOnlyMemory<char> memory) => new(memory.Span);
    private static ReadOnlyMemory<char> ConvertStringToRomOfChar(string str) => str.AsMemory();
    
    private static byte[] ConvertRomOfByteToByteArray(ReadOnlyMemory<byte> memory) => memory.ToArray();
    private static ReadOnlyMemory<byte> ConvertByteArrayToRomOfByte(byte[] array) => array.AsMemory();
    
    private static long ConvertTimeOnlyToLong(TimeOnly time) => time.ToTimeSpan().Ticks;
    private static TimeOnly ConvertLongToTimeOnly(long ticks) => TimeOnly.FromTimeSpan(TimeSpan.FromTicks(ticks));
    
    readonly record struct TypePair(Type From, Type To);

    private static MethodInfo M(string name) => typeof(DataConverter).GetMethod(
        name, BindingFlags.NonPublic | BindingFlags.Static)!;

    private static KeyValuePair<TypePair, MethodInfo> E<TFrom, TTo>(string methodName) {
        return new KeyValuePair<TypePair, MethodInfo>(new TypePair(typeof(TFrom), typeof(TTo)), M(methodName));
    }

    private static readonly Dictionary<TypePair, MethodInfo> _conversionMethods = new(
        new List<KeyValuePair<TypePair, MethodInfo>> {
            
            E<ROMC, string>(nameof(ConvertRomOfCharToString)),
            E<string, ROMC>(nameof(ConvertStringToRomOfChar)),
            E<ROMB, byte[]>(nameof(ConvertRomOfByteToByteArray)),
            E<byte[], ROMB>(nameof(ConvertByteArrayToRomOfByte)),
            E<TimeOnly, long>(nameof(ConvertTimeOnlyToLong)),
            E<long, TimeOnly>(nameof(ConvertLongToTimeOnly)),
        });

    #endregion

    public static Expression Convert(Expression source, Type sourceType, Type targetType) {
        
        Type sourceBareType = sourceType.IsNullable() ? sourceType.GetNonNullable() : sourceType;
        Type targetBareType = targetType.IsNullable() ? targetType.GetNonNullable() : targetType;
        
        if(sourceBareType == targetBareType)
            return Expression.Convert(source, targetType);

        if(_conversionMethods.TryGetValue(new TypePair(sourceBareType, targetBareType), out MethodInfo? method)) {
            Expression result = Expression.Call(method, source);
            
            if(targetType != targetBareType)
                result = Expression.Convert(result, targetType);

            return result;
        }
        
        // object boxing (untyped path)
        if(targetType == typeof(object)) {
            // respect conversion configuration for untyped path
            if(ParquetOptions.PreferUntypedByteArray && sourceType == typeof(ReadOnlyMemory<byte>))
                return Expression.Call(M(nameof(ConvertRomOfByteToByteArray)), source);

            if(ParquetOptions.PreferUntypedString && sourceType == typeof(ReadOnlyMemory<char>))
                return Expression.Call(M(nameof(ConvertRomOfCharToString)), source);

            return Expression.Convert(source, typeof(object));
        }

        return Expression.Convert(source, targetType);
    }

    public static bool TryConvert(Type sourceType, object source, Type targetType, out object? target) {
        if(targetType == typeof(ReadOnlyMemory<char>) || targetType == typeof(ReadOnlyMemory<char>?)) {
            if(source is string s) {
                target = s.AsNullableReadOnlyMemory();
                return true;
            }
        }

        target = default!;
        return false;
    }
}
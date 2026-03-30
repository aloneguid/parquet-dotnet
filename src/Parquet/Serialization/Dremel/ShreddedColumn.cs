using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Schema;

namespace Parquet.Serialization.Dremel; 

class ShreddedColumn {

    private readonly IList _data;

    public ShreddedColumn(IList data, List<int>? definitionLevels, List<int>? repetitionLevels) {
        _data = data;
        DefinitionLevels = definitionLevels;
        RepetitionLevels = repetitionLevels;
    }

    public List<int>? DefinitionLevels { get; set; }

    public List<int>? RepetitionLevels { get; set; }

    public IList Data => _data;

    public async Task CallWriteAsync(DataField df, ParquetRowGroupWriter rgw) {

        if(!_data.GetType().IsGenericList(out Type valueType)) {
            throw new InvalidCastException();
        }

        IList valuesList = _data;
        if(ConvertListIfRequired(out IList convertedList, out Type convertedType)) {
            valueType = convertedType;
            valuesList = convertedList;
        }

        // get all required parts to make the call
        object valuesMemory = GetValuesMemoryUnsafe(valuesList, valueType);
        ReadOnlyMemory<int>? definitionLevels = GetMemoryUnsafe(DefinitionLevels);
        ReadOnlyMemory<int>? repetitionLevels = GetMemoryUnsafe(RepetitionLevels);

        // call WriteAsync via reflection, as it's generic and we only have the type at runtime
        MethodInfo? method = typeof(ParquetRowGroupWriter)
            .GetMethod(nameof(ParquetRowGroupWriter.WriteAsyncAllParts), BindingFlags.NonPublic | BindingFlags.Instance);

        if(method == null) {
            throw new InvalidOperationException($"can't find {nameof(ParquetRowGroupWriter.WriteAsyncAllParts)} method on {nameof(ParquetRowGroupWriter)}");
        }

        MethodInfo genericMethod = method.MakeGenericMethod(valueType);
        Task writeTask = (Task)genericMethod.Invoke(rgw, [df, valuesMemory, definitionLevels, repetitionLevels, CancellationToken.None])!;
        await writeTask.ConfigureAwait(false);
    }

    private bool ConvertListIfRequired(out IList dest, out Type destType) {
        if(_data is IList<string> stringList) {
            dest = _data.Cast<string>().Select(s => s.AsReadOnlyMemory()).ToList();
            destType = typeof(ReadOnlyMemory<char>);
            return true;
        }

        dest = _data;
        destType = typeof(void);
        return false;

    }

    private static object GetValuesMemoryUnsafe(IList baseRef, Type valueType) {
        Type listType = typeof(List<>).MakeGenericType(valueType);

        // call GetMemoryUnsafe via reflection, as it's generic and we only have the type at runtime
        MethodInfo? method = typeof(ShreddedColumn).GetMethod(nameof(GetMemoryUnsafe),
            BindingFlags.NonPublic | BindingFlags.Static);

        MethodInfo genericMethod = method!.MakeGenericMethod(valueType);
        return genericMethod.Invoke(null, new object?[] { baseRef })!;
    }

    private static ReadOnlyMemory<T>? GetMemoryUnsafe<T>(List<T>? list) where T: struct {
        if(list == null)
            return null;
        FieldInfo? field = typeof(List<T>).GetField("_items",
            BindingFlags.NonPublic | BindingFlags.Instance);
        if(field == null) {
            throw new InvalidOperationException($"can't unsafely get raw array");
        }
        T[]? values = field.GetValue(list) as T[];
        if(values == null) {
            throw new InvalidOperationException($"can't unsafely get raw array");
        }
        return new ReadOnlyMemory<T>(values, 0, list.Count);
    }
}

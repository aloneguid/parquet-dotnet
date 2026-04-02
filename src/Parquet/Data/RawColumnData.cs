using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Data;

/// <summary>
/// 
/// </summary>
/// <typeparam name="T"></typeparam>
public class RawColumnData<T> : IDisposable where T : struct {
    private readonly IMemoryOwner<T> _values;
    private readonly IMemoryOwner<int>? _definitionLevels;
    private readonly IMemoryOwner<int>? _repetitionLevels;

    /// <summary>
    /// 
    /// </summary>
    /// <param name="values"></param>
    /// <param name="definitionLevels"></param>
    /// <param name="repetitionLevels"></param>
    public RawColumnData(IMemoryOwner<T> values, IMemoryOwner<int>? definitionLevels, IMemoryOwner<int>? repetitionLevels) {
        _values = values;
        _definitionLevels = definitionLevels;
        _repetitionLevels = repetitionLevels;
    }

    /// <summary>
    /// 
    /// </summary>
    public Span<T> Values => _values.Memory.Span;

    /// <summary>
    /// 
    /// </summary>
    public Span<int> DefinitionLevels {
        get {
            if(_definitionLevels == null)
                throw new InvalidOperationException("definition levels are not present for this column");

            return _definitionLevels.Memory.Span;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public void Dispose() {
        _values.Dispose();
        _definitionLevels?.Dispose();
        _repetitionLevels?.Dispose();
    }
}

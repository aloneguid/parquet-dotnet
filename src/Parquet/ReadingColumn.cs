using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet;

/// <summary>
/// Just like <see cref="WritingColumn{T}"/>, contains intermediate data structures used during column reading.
/// Keps separate as trying to share code between reading and writing is more trouble than it's worth.
/// </summary>
class ReadingColumn<T> : IDisposable where T : struct {
    public void Dispose() {

    }
}

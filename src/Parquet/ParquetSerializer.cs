using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet {

    /// <summary>
    /// High level serialization API.
    /// Called ParquetSerializer due to efforts to make it similar to JsonSerializer (https://learn.microsoft.com/en-us/dotnet/standard/serialization/system-text-json/how-to?pivots=dotnet-7-0).
    /// This class deprecates row-based API from v4 by simply supporting Dictionary serialization
    /// </summary>
    public static class ParquetSerializer {
    }
}

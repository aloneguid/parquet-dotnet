# Utilities

%product% provides out-of-the-box helpers that came out of frequently asked questions or repeated scenarios authors had to work with.

All the utilities reside in `Parquet.Utils` namespace.

## Converting to flat format

 `FlatTableConverter` helps to represent data in row format, however before using it consider the following:

- Not all parquet files can be represented in flat format. For instance, if a parquet file contains a list of structures, it cannot be represented in row format, therefore you cannot convert it to flat format.
- Complex data (structs, maps etc.) will be simply skipped in current implementation. Support will be added in the future.

To minimise external dependencies, %product% does not provide any implementations of a specific data format like CSV, TSV etc., but you can implement it yourself easily by deriving from a helper class.

The example below shows how to convert a parquet file to CSV using free open-source [CsvHelper](https://joshclose.github.io/CsvHelper/) library:

```C#
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using Parquet.Utils;
using Parquet.Schema;

namespace Parquet.Floor.Controllers {
    /// <summary>
    /// Example use of CsvHelper to convert Parquet to CSV.
    /// </summary>
    class ParquetToCsvConverter : FlatTableConverter {

        private readonly StreamWriter _sw;
        private readonly CsvWriter _csv;
        
        public ParquetToCsvConverter(Stream parquetStream, string csvFilePath, ParquetOptions? options = null) 
            : base(parquetStream, options) {
            _sw = new StreamWriter(csvFilePath);
            _csv = new CsvWriter(_sw, CultureInfo.InvariantCulture);
        }

        protected override Task NewRow() {
            return _csv.NextRecordAsync();
        }
        
        protected override async Task WriteCellAsync(Field f, object? value, CancellationToken cancellationToken = default) {
            _csv.WriteField(value?.ToString() ?? "");
        }
        
        protected override async Task WriteHeaderAsync(ParquetSchema schema, CancellationToken cancellationToken = default) {
            foreach(Field f in schema.Fields) {
                _csv.WriteField(f.Name);
            }
        }

        public override void Dispose() {
            _csv.Dispose();
            _sw.Dispose();

            base.Dispose();
        }
    }
}
```

You can then call this converter like this:

```C#
  using(System.IO.FileStream stream = System.IO.File.OpenRead(_parquetFilePath)) {
      using(var converter = new ParquetToCsvConverter(stream, _csvFilePath)) {
          await converter.ConvertAsync();
      }
}
```

In fact, [Parquet Floor](parquet-floor.md) uses exactly the same approach to convert parquet files to CSV.

## Merging files into one

`FileMerger` helps to merge multiple parquet files into one. The files must have the same schema. File merger operates in one of two modes:

1. Merging files. The resulting file will contain copies of all the row groups from all the files. File size will be probably similar to the sum of the sizes of the source files. This is suitable for larger row groups.
2. Mering row groups. The resulting file will contain only single row group, which will contain all the values from all the row groups in the source files. Needless to say, the values will be re-compressed logically and physically, which can save a lot of disk space. This is suitable for small row groups.

```C#
using Parquet.Utils;

using var merger = new FileMerger(...input...);
await merger.MergeFilesAsync(...output...);

```
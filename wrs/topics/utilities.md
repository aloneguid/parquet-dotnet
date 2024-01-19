# Utilities

## Converting to flat format

%product% provides out-of-the-box helpers to represent data in row format, however before using it consider the following:

- Not all parquet files can be represented in flat format. For instance, if a parquet file contains a list of structures, it cannot be represented in row format, therefore you cannot convert it to flat format.

- Complex data will be simply skipped.

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
using Parquet.Converters;
using Parquet.Schema;

namespace Parquet.Floor.Controllers {
    /// <summary>
    /// Example use of CsvHelper to convert Parquet to CSV.
    /// </summary>
    class ParquetToCsvConverter : ParquetToFlatTableConverter {

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
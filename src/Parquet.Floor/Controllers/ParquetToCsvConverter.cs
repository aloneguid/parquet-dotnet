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
    /// 
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
        protected override async Task WriteCellAsync(DataField df, object? value, CancellationToken cancellationToken = default) {
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

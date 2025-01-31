using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using Parquet.Schema;
using Parquet.Serialization;
using Parquet.Utils;

namespace Parquet.Floor.Controllers {
    /// <summary>
    /// Example use of CsvHelper to convert Parquet to CSV.
    /// 
    /// </summary>
    class ParquetToCsvConverter : FlatTableConverter {

        private readonly StreamWriter _sw;
        private readonly CsvWriter _csv;
        public ParquetToCsvConverter(Stream parquetStream, string csvFilePath, ParquetSerializerOptions? options = null) 
            : base(parquetStream, options) {
            _sw = new StreamWriter(csvFilePath);
            _csv = new CsvWriter(_sw, CultureInfo.InvariantCulture);
        }

        protected override Task NewRow() {
            return _csv.NextRecordAsync();
        }
        protected override Task WriteCellAsync(Field f, object? value, CancellationToken cancellationToken = default) {
            _csv.WriteField(value?.ToString() ?? "");
            return Task.CompletedTask;
        }
        protected override Task WriteHeaderAsync(ParquetSchema schema, CancellationToken cancellationToken = default) {
            foreach(Field f in schema.Fields) {
                _csv.WriteField(f.Name);
            }
            return Task.CompletedTask;
        }

        public override void Dispose() {
            _csv.Dispose();
            _sw.Dispose();

            base.Dispose();
        }
    }
}

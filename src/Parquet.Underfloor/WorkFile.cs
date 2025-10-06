using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet.Underfloor {

    /// <summary>
    /// This keeps parquet file open for the duration of the session, in order to read parts of it on demand.
    /// </summary>
    class WorkFile : IAsyncDisposable {

        private Stream? _stream;
        private ParquetReader? _reader;
        private ParquetSchema? _schema;
        private FileMetaData? _meta;

        public static async Task<WorkFile> CreateAsync(string? path) {
            var wf = new WorkFile();
            if(path == null) {
                wf.FilePath = "";
                wf.FileName = "?";
                wf.ErrorMessage = "No file path provided.";
            } else {
                wf.FilePath = path;
                wf.FileName = Path.GetFileName(path) ?? "?";
                wf.SizeDisplay = new FileInfo(path).Length.ToFileSizeUiString();

                wf._stream = System.IO.File.OpenRead(path);
                wf._reader = await ParquetReader.CreateAsync(wf._stream);

                wf.Metadata = wf._reader.Metadata;
                wf.Schema = wf._reader.Schema;
                wf.RowGroups = wf._reader.RowGroups;
            }

            return wf;
        }

        public string? ErrorMessage { get; set; }

        public string? FileName { get; set; }

        public string? FilePath { get; set; }

        public string? SizeDisplay { get; set; }

        public FileMetaData? Metadata {
            get => _meta;
            set {
                _meta = value;

                if(value != null) {
                    RowCountDisplay = value.NumRows.ToString("N0");
                    VersionDisplay = value.Version.ToString();
                    RowGroupCountDisplay = value.RowGroups.Count.ToString("N0");
                }
            }
        }

        public string? RowCountDisplay { get; set; }

        public string? RowGroupCountDisplay { get; set; }

        public string? VersionDisplay { get; set; }

        public ParquetSchema? Schema {
            get => _schema;
            set {
                _schema = value;

                if(value != null) {
                    ColumnCountDisplay = value.DataFields.Length.ToString("N0");
                    Columns = value.DataFields.Select(f => f.Name).ToArray();
                    ColumnsDisplay = ["#", .. Columns];

                }
            }
        }

        public string? ColumnCountDisplay { get; set; }

        public string[]? Columns { get; set; }

        public string[]? ColumnsDisplay { get; set; }

        public IReadOnlyList<IParquetRowGroupReader>? RowGroups { get; set; }

        public async ValueTask DisposeAsync() {
            if(_stream != null) {
                await _stream.DisposeAsync();
                _stream = null;
            }
        }
    }
}

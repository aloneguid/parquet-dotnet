using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Tasks;
using Parquet.Meta;
using Parquet.Schema;
using Parquet.Serialization;

namespace Parquet.Underfloor {
    class FileData {

        private ParquetSchema? _schema;
        private FileMetaData? _meta;

        public FileData(string? filePath) {
            if(filePath == null) {
                FilePath = "";
                FileName = "?";
                ErrorMessage = "No file path provided.";
            } else {
                FilePath = filePath;
                FileName = Path.GetFileName(filePath) ?? "?";
                SizeDisplay = new FileInfo(filePath).Length.ToFileSizeUiString();
            }
        }

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

        public FileMetaData? Metadata {
            get => _meta;
            set {
                _meta = value;

                if(value != null) {
                    RowCountDisplay = value.NumRows.ToString("N0");
                    VersionDisplay = value.Version.ToString();
                }
            }
        }

        public string FileName { get; init; }

        public string FilePath { get; init; }

        public IReadOnlyList<IParquetRowGroupReader>? RowGroups { get; set; }

        public Dictionary<string, string>? CustomMetadata { get; set; }

        public string? SizeDisplay { get; set; }

        public string? RowCountDisplay { get; set; }

        public string? RowGroupCountDisplay { get; set; }

        public string? VersionDisplay { get; set; }

        public string? ColumnCountDisplay { get; set; }

        public string[]? Columns { get; set; }

        public string[]? ColumnsDisplay { get; set; }

        public string? ErrorMessage { get; set; }
    }
}

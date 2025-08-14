using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Parquet.Meta;
using Parquet.Schema;
using Parquet.Serialization;

namespace Parquet.Underfloor {
    class FileData {
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

        public string FileName { get; init; }

        public string FilePath { get; init; }

        public ParquetSchema? Schema { get; set; }

        public FileMetaData? FileMeta { get; set; }

        public IReadOnlyList<IParquetRowGroupReader>? RowGroups { get; set; }

        public Dictionary<string, string>? CustomMetadata { get; set; }

        public string? SizeDisplay { get; set; }

        public string? RowCountDisplay { get; set; }

        public string? RowGroupCountDisplay { get; set; }

        public string? VersionDisplay { get; set; }

        public string? ColumnCountDisplay { get; set; }

        public bool IsDataLoading { get; set; } = false;

        public bool HasLoaded { get; set; } = false;

        public ParquetSerializer.UntypedResult? Data { get; set; }

        public string[]? Columns { get; set; }

        public string? ErrorMessage { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CommunityToolkit.Mvvm.ComponentModel;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet.Floor.ViewModels {
    public partial class FileViewModel : ViewModelBase {

        [ObservableProperty]
        private string? _path;

        [ObservableProperty]
        private ParquetSchema? _schema;

        [ObservableProperty]
        private Dictionary<string, string>? _customMetadata;

        [ObservableProperty]
        private int _rowGroupCount;

        [ObservableProperty]
        private long _rowCount;

        [ObservableProperty]
        private FileMetaData? _metadata;
    }
}

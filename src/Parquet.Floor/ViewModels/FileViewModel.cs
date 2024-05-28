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
        private string? _name;

        [ObservableProperty]
        private ParquetSchema? _schema;

        [ObservableProperty]
        private Dictionary<string, string>? _customMetadata;

        [ObservableProperty]
        private int _rowGroupCount;

        [ObservableProperty]
        private long _rowCount;

        [ObservableProperty]
        private long _loadTimeMs;

        [ObservableProperty]
        private string? _createdBy;

        [ObservableProperty]
        private FileMetaDataViewModel? _metadata;
    }
}

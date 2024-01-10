using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using CommunityToolkit.Mvvm.ComponentModel;
using Parquet.Serialization;
using Parquet.Schema;
using Avalonia.Controls;
using Parquet.Floor.Views;
using System.Collections.ObjectModel;
using System.Linq;
using Parquet.Meta;
using Avalonia.Threading;

namespace Parquet.Floor.ViewModels;

public class CellModel {
    public CellModel(Parquet.Schema.Field f, Dictionary<string, object> row) {

    }
}

public partial class DataViewModel : ViewModelBase {

    [ObservableProperty]
    private FileViewModel? _file;

    [ObservableProperty]
    private IList<Dictionary<string, object>>? _data;

    public DataViewModel() {
#if DEBUG
        if(Design.IsDesignMode) {
            File = new FileViewModel {
                Schema = DesignData.Schema,
                RowCount = 1012,
                RowGroupCount = 3,
                CreatedBy = "Parquet.Floor",
            };
            Data = DesignData.Data;
        }
#endif
    }

    public async Task InitReaderAsync(FileViewModel? file, Stream fileStream) {
        ParquetSerializer.UntypedResult fd = await ParquetSerializer.DeserializeAsync(fileStream);

        Dispatcher.UIThread.Invoke(() => {
            File = file;
            Data = fd.Data;
        });
    }
}

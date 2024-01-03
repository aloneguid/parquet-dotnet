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

namespace Parquet.Floor.ViewModels;

public class CellModel {
    public CellModel(Parquet.Schema.Field f, Dictionary<string, object> row) {

    }
}

public partial class DataViewModel : ViewModelBase {

    [ObservableProperty]
    private ParquetSchema? _schema;

    [ObservableProperty]
    private IList<Dictionary<string, object>>? _data;

    public DataViewModel() {
#if DEBUG
        if(Design.IsDesignMode) {
            Schema = DesignData.Schema;
            Data = DesignData.Data;
        }
#endif
    }

    public async Task InitReaderAsync(Stream fileStream) {
        ParquetSerializer.UntypedResult fd = await ParquetSerializer.DeserializeAsync(fileStream);
        Schema = fd.Schema;
        Data = fd.Data;
    }
}

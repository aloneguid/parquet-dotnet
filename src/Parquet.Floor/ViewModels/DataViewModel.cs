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

public interface IDataViewGridView {
    string? GetFormattedSelectedRowText(bool copyToClipboard);
}

public partial class DataViewModel : ViewModelBase {

    [ObservableProperty]
    private FileViewModel? _file;

    [ObservableProperty]
    private IList<Dictionary<string, object>>? _data;

    [ObservableProperty]
    private bool _hasError;

    [ObservableProperty]
    private string? _errorMessage;

    [ObservableProperty]
    private string? _errorDetails;

    [ObservableProperty]
    private bool _showErrorDetails;

    public IDataViewGridView? DataGridView { get; set; }

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

        IList<Dictionary<string, object>>? data = null;
        HasError = false;
        ErrorMessage = null;
        ErrorDetails = null;

        try {
            ParquetSerializer.UntypedResult fd = await ParquetSerializer.DeserializeAsync(
                fileStream,
                new ParquetSerializerOptions {
                    ParquetOptions = new ParquetOptions {
                        TreatByteArrayAsString = true
                    }
                });
            data = fd.Data;
        } catch(Exception ex) {
            HasError = true;
            ErrorMessage = ex.Message;
            ErrorDetails = ex.ToString();
            Tracker.Instance.Track("openFileError", new Dictionary<string, string> {
                { "message", ex.Message },
                { "details", ex.ToString() }
            });
        }

        Dispatcher.UIThread.Invoke(() => {
            File = file;
            Data = data;
        });
    }

    public void CopyRow() {
        if(DataGridView == null)
            return;

        DataGridView.GetFormattedSelectedRowText(true);
    }
}

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Controls.Models.TreeDataGrid;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet.Floor.ViewModels;

public partial class MainViewModel : ViewModelBase {

    private Stream? _fileStream;

    [ObservableProperty]
    private string? _path = "test.parquet";

    [ObservableProperty]
    private bool _hasError;

    [ObservableProperty]
    private bool _showErrorDetails;

    [ObservableProperty]
    private string _errorMessage;

    [ObservableProperty]
    private string _errorDetails;

    public SchemaViewModel Schema { get; } = new SchemaViewModel();

    public DataViewModel Data { get; } = new DataViewModel();

    public MainViewModel() {
        string[] args = Environment.GetCommandLineArgs();

        if(Design.IsDesignMode) {
            LoadDesignData();
        } else {
            if(args.Length > 1) {
                Path = args[1];
                Load(Path);
            }
        }
    }

    private void LoadDesignData() {
        Path = "design.parquet";
        ErrorMessage = "This is a design-time error message.";
        ErrorDetails = "This is a design-time error details message.\nLine 2";
    }

    public void Load(string path) {
        Task.Run(() => LoadAsync(path));
    }

    private async Task LoadAsync(string path) {
        if(!System.IO.File.Exists(path))
            return;

        await LoadAsync(System.IO.File.OpenRead(path));
    }

    public async Task LoadAsync(Stream fileStream) {

        if(_fileStream != null) {
            _fileStream.Close();
            _fileStream.Dispose();
            _fileStream = null;
        }

        _fileStream = fileStream;

        HasError = false;
        ErrorMessage = "";
        ErrorDetails = "";
        ShowErrorDetails = false;

        try {
            using ParquetReader reader = await ParquetReader.CreateAsync(_fileStream);
            Schema.InitSchema(reader.Schema);
            Data.InitReader(_fileStream);

            // dispatch to UI thread
            Dispatcher.UIThread.Invoke(() => {
                // todo
            });

        } catch(Exception ex) {
            HasError = true;
            ErrorMessage = ex.Message;
            ErrorDetails = ex.ToString();
        }
    }

}

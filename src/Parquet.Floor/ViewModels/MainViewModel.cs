using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Reflection.PortableExecutable;
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
    private string? _title;

    [ObservableProperty]
    private string? _subTitle;

    [ObservableProperty]
    private FileViewModel? _file;

    [ObservableProperty]
    private bool _hasError;

    [ObservableProperty]
    private bool _showErrorDetails;

    [ObservableProperty]
    private string? _errorMessage;

    [ObservableProperty]
    private string? _errorDetails;

    public SchemaViewModel Schema { get; } = new SchemaViewModel();

    public DataViewModel Data { get; } = new DataViewModel();

    public MainViewModel() {

        Title = "Parquet Floor";
        SubTitle = Parquet.Globals.Version;

        string[] args = Environment.GetCommandLineArgs();

        if(Design.IsDesignMode) {
            LoadDesignData();
        } else {
            if(args.Length > 1) {
                LoadFromFile(args[1]);
            }
        }
    }

    private void LoadDesignData() {
        File = new FileViewModel {
            Path = "design.parquet",
            CreatedBy = "Parquet.Floor",
        };
        ErrorMessage = "This is a design-time error message.";
        ErrorDetails = "This is a design-time error details message.\nLine 2";
    }

    public void LoadFromFile(string path) {
        Task.Run(() => LoadFromFileAsync(path));
    }

    private async Task LoadFromFileAsync(string path) {
        if(!System.IO.File.Exists(path))
            return;

        await LoadAsync(System.IO.File.OpenRead(path));
    }

    private async Task LoadAsync(Stream fileStream) {

        if(_fileStream != null) {
            _fileStream.Close();
            _fileStream.Dispose();
            _fileStream = null;
        }

        _fileStream = fileStream;

        HasError = false;
        ErrorMessage = null;
        ErrorDetails = null;
        ShowErrorDetails = false;

        try {
            using(ParquetReader reader = await ParquetReader.CreateAsync(_fileStream)) {
                File = new FileViewModel {
                    Schema = reader.Schema,
                    CustomMetadata = reader.CustomMetadata,
                    RowGroupCount = reader.RowGroupCount,
                    Metadata = new FileMetaDataViewModel(reader.Metadata),
                    RowCount = reader.Metadata?.NumRows ?? 0,
                    CreatedBy = reader.Metadata?.CreatedBy
                };
            }
            Schema.InitSchema(File.Schema);
            Data.File = File;
            await Data.InitReaderAsync(_fileStream);

        } catch(Exception ex) {
            HasError = true;
            ErrorMessage = ex.Message;
            ErrorDetails = ex.ToString();
        }
    }

}

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
using CommunityToolkit.Mvvm.Messaging;
using NetBox.Performance;
using Parquet.Floor.Controllers;
using Parquet.Floor.Messages;
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
    private bool _isLoading;

    [ObservableProperty]
    private FileViewModel? _file;

    [ObservableProperty]
    private bool _hasFile;

    [ObservableProperty]
    private bool _hasError;

    [ObservableProperty]
    private bool _showErrorDetails;

    [ObservableProperty]
    private string? _errorMessage;

    [ObservableProperty]
    private string? _errorDetails;

    [ObservableProperty]
    private string? _newerVersionNumber;

    public string? LatestParquetPath;

    public SchemaViewModel Schema { get; } = new SchemaViewModel();

    public DataViewModel Data { get; } = new DataViewModel();

    public event Action<string>? OnNewVersionAvailable;

    public MainViewModel() {

        string version = Parquet.Globals.Version;
#if DEBUG
        version = "Next";
#endif

        Title = $"Parquet Floor v{version}";
        SubTitle = "no file loaded";

        string[] args = Environment.GetCommandLineArgs();

        if(Design.IsDesignMode) {
            LoadDesignData();
        } else {
            if(args.Length > 1) {
                LoadFromFile(args[1]);
            }
        }

        WeakReferenceMessenger.Default.Register<FileOpenMessage>(this, (r, m) => {
            LoadFromFile(m.Value);
        });

        CheckForUpdates().Forget();
    }

    private async Task CheckForUpdates() {
        var gh = new GitHub();
        try {
            GitHub.Release latestRelease = await gh.GetLatestRelease("aloneguid", "parquet-dotnet");
            if(latestRelease.Name != Globals.Version) {
                NewerVersionNumber = latestRelease.Name;
                OnNewVersionAvailable?.Invoke(NewerVersionNumber!);
            }
        }
        catch(Exception ex) {
            Tracker.Instance.Track("updateCheckFailed", new Dictionary<string, string> {
                { "message", ex.Message }
            });
        }
    }

    public void OpenHomePage() {
        "https://github.com/aloneguid/parquet-dotnet".OpenInBrowser();
        Tracker.Instance.Track("openHomePage");
    }

    private void LoadDesignData() {
        File = new FileViewModel {
            Name = "design.parquet",
            CreatedBy = "Parquet.Floor",
        };
        ErrorMessage = "This is a design-time error message.";
        ErrorDetails = "This is a design-time error details message.\nLine 2";
        HasFile = true;
    }

    public void LoadFromFile(string path) {
        LatestParquetPath = path;

        string fileName = new FileInfo(path).Name;
        const int maxLength = 30;
        bool isTooLong = fileName.Length > maxLength;
        SubTitle = isTooLong
            ? $"{fileName.Substring(0, maxLength)}..."
            : fileName;

        Task.Run(() => LoadFromFileAsync(path));
    }

    private async Task LoadFromFileAsync(string path) {
        if(!System.IO.File.Exists(path))
            return;

        await LoadAsync(new FileInfo(path).Name, System.IO.File.OpenRead(path));
    }

    #region [ Command bindings ]

    public void ReloadFile() {
        if(LatestParquetPath != null) {
            LoadFromFile(LatestParquetPath);
        }
    }

    public bool CanReloadFile() => LatestParquetPath != null;

    public void ConvertToCsv(object? toClipboard) {
        if(LatestParquetPath == null)
            return;

        bool useClipboard = toClipboard is bool b && b;
    }

    #endregion

    private async Task LoadAsync(string name, Stream fileStream) {

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
        IsLoading = true;

        try {
            using(var ts = new TimeMeasure()) {
                using(ParquetReader reader = await ParquetReader.CreateAsync(_fileStream)) {
                    File = new FileViewModel {
                        Name = name,
                        Schema = reader.Schema,
                        CustomMetadata = reader.CustomMetadata,
                        RowGroupCount = reader.RowGroupCount,
                        Metadata = new FileMetaDataViewModel(reader.Metadata),
                        RowCount = reader.Metadata?.NumRows ?? 0,
                        CreatedBy = reader.Metadata?.CreatedBy
                    };
                }
                HasFile = true;
                Schema.InitSchema(File.Schema);
                await Data.InitReaderAsync(File, _fileStream);
                File.LoadTimeMs = ts.ElapsedMilliseconds;
            }

            Tracker.Instance.Track("fileOpen", new Dictionary<string, string> {
                { "rowCount", File.RowCount.ToString() },
                { "rowGroupCount", File.RowGroupCount.ToString() },
                { "columnCount", File.Schema.Fields.Count.ToString() },
                { "createdBy", File.CreatedBy ?? "unknown" },
                { "size", _fileStream.Length.ToString() },
            });


        } catch(Exception ex) {
            HasError = true;
            ErrorMessage = ex.Message;
            ErrorDetails = ex.ToString();
            Tracker.Instance.Track("openFileFatal", new Dictionary<string, string> {
                { "message", ex.Message },
                { "details", ex.ToString() }
            });
        } finally {
            IsLoading = false;
        }
    }

}

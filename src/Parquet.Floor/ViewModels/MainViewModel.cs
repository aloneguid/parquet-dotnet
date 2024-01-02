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
    public string Greeting => "Welcome to Avalonia!";

    [ObservableProperty]
    private string? _path = "test.parquet";

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
        try {
            using ParquetReader reader = await ParquetReader.CreateAsync(fileStream);
            Schema.InitSchema(reader.Schema);
            Data.InitReader(fileStream);

            // dispatch to UI thread
            Dispatcher.UIThread.Invoke(() => {
                // todo
            });

        } catch(Exception ex) {
            Console.WriteLine(ex);
        } finally {
            // fileStream.Close();
        }

    }

}

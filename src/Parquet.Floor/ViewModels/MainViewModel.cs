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

    private void Load(string path) {
        Task.Run(() => LoadAsync(path));
    }

    private async Task LoadAsync(string path) {
        try {
            if(!System.IO.File.Exists(path))
                return;
            using FileStream fs = System.IO.File.OpenRead(path);
            using ParquetReader reader = await ParquetReader.CreateAsync(fs);
            Schema.InitSchema(reader.Schema);

            // dispatch to UI thread
            Dispatcher.UIThread.Invoke(() => {
                // todo
            });

        } catch(Exception ex) {
            Console.WriteLine(ex);
        }
    }

}

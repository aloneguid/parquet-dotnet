using System.Collections.Generic;
using System.Linq;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Markup.Xaml;
using Avalonia.Platform.Storage;
using Avalonia.Threading;
using Parquet.Floor.ViewModels.Utils;

namespace Parquet.Floor.Views.Utils;

public partial class FileMergerUtil : UserControl {

    private readonly FileMergerViewModel _vm = new();

    public FileMergerUtil() {
        InitializeComponent();

        this.DataContext = _vm;
    }

    private async void SelectSourceFolderClick(object? sender, Avalonia.Interactivity.RoutedEventArgs e) {
        var topLevel = TopLevel.GetTopLevel(this);
        if(topLevel == null)
            return;

        // Start async operation to open the dialog.
        IReadOnlyList<IStorageFolder> folders = await topLevel.StorageProvider.OpenFolderPickerAsync(new FolderPickerOpenOptions {
            Title = "Pick folder to merge",
            AllowMultiple = false
        });

        if(folders.Count >= 1) {
            Dispatcher.UIThread.Invoke(() => {
                _vm.SetInput(folders[0].Path.LocalPath);
            });
        }
    }

    private async void SelectSourceFilesClick(object? sender, Avalonia.Interactivity.RoutedEventArgs e) {
        var topLevel = TopLevel.GetTopLevel(this);
        if(topLevel == null)
            return;

        // Start async operation to open the dialog.
        IReadOnlyList<IStorageFile> files = await topLevel.StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions {
            Title = "Pick files to merge",
            AllowMultiple = true
        });

        if(files.Count >= 1) {
            Dispatcher.UIThread.Invoke(() => {
                _vm.SetInput(files.Select(f => f.Path.LocalPath));
            });
        }
    }


    private async void SelectTargetClick(object? sender, Avalonia.Interactivity.RoutedEventArgs e) {
        var topLevel = TopLevel.GetTopLevel(this);
        if(topLevel == null)
            return;

        IStorageFile? file = await topLevel.StorageProvider.SaveFilePickerAsync(new FilePickerSaveOptions {
            Title = "Target file name",
            DefaultExtension = "parquet",
        });

        if(file != null) {
            Dispatcher.UIThread.Invoke(() => {
                _vm.SetOutput(file.Path.LocalPath);
            });
        }
    }
}
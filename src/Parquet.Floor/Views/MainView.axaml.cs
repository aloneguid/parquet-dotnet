using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Controls.Primitives;
using Avalonia.Platform.Storage;
using Parquet.Floor.ViewModels;

namespace Parquet.Floor.Views;

public partial class MainView : UserControl {
    public MainView() {
        InitializeComponent();
    }

    public MainViewModel ViewModel => (MainViewModel)DataContext!;

    private async void OpenFile_Click(object? sender, Avalonia.Interactivity.RoutedEventArgs e) {

        FlyoutBase.ShowAttachedFlyout(this);

        // see https://docs.avaloniaui.net/docs/basics/user-interface/file-dialogs

        // Get top level from the current control. Alternatively, you can use Window reference instead.
        var topLevel = TopLevel.GetTopLevel(this);
        if(topLevel == null) return;

        // Start async operation to open the dialog.
        IReadOnlyList<IStorageFile> files = await topLevel.StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions {
            Title = "Open Parquet File",
            AllowMultiple = false
        });

        if(files.Count >= 1) {
            ViewModel.LoadFromFile(files[0].Path.LocalPath);
        }
    }
}

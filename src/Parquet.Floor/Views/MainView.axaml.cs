using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Threading.Tasks;
using ActiproSoftware.Logging;
using ActiproSoftware.UI.Avalonia.Themes;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.Notifications;
using Avalonia.Controls.Primitives;
using Avalonia.Platform.Storage;
using Parquet.Floor.ViewModels;
using Parquet.Floor.Views.Utils;
using Parquet.Utils;

namespace Parquet.Floor.Views;

public partial class MainView : UserControl {

    private WindowNotificationManager? _notificationManager;

    public MainView() {
        InitializeComponent();
    }

    public MainViewModel ViewModel => (MainViewModel)DataContext!;

    protected override void OnAttachedToVisualTree(VisualTreeAttachmentEventArgs e) {
        _notificationManager ??= new WindowNotificationManager(TopLevel.GetTopLevel(this));
        base.OnAttachedToVisualTree(e);
    }

    void OpenLatestReleasePage() {
        "https://github.com/aloneguid/parquet-dotnet/releases".OpenInBrowser();
        Tracker.Instance.Track("openLatestReleasePage");
    }

    protected override void OnDataContextChanged(EventArgs e) {
        if(ViewModel != null) {
            ViewModel.OnNewVersionAvailable += (v) => {
                if(_notificationManager != null) {
                    _notificationManager.Show(
                        new Notification("New version available",
                        $"Version {v} is available. Please download it from the GitHub releases page.",
                            NotificationType.Information, onClick: () => {
                                OpenLatestReleasePage();
                            }));
                }
            };
        }
    }

    private async void OpenFile_Click(object? sender, Avalonia.Interactivity.RoutedEventArgs e) {

        FlyoutBase.ShowAttachedFlyout(this);

        // see https://docs.avaloniaui.net/docs/basics/user-interface/file-dialogs

        // Get top level from the current control. Alternatively, you can use Window reference instead.
        var topLevel = TopLevel.GetTopLevel(this);
        if(topLevel == null)
            return;

        // Start async operation to open the dialog.
        IReadOnlyList<IStorageFile> files = await topLevel.StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions {
            Title = "Open Parquet File",
            AllowMultiple = false
        });

        if(files.Count >= 1) {
            ViewModel.LoadFromFile(files[0].Path.LocalPath);
        }
    }

    private async void ConvertToCsv_Click(object? sender, Avalonia.Interactivity.RoutedEventArgs e) {

        var topLevel = TopLevel.GetTopLevel(this);
        if(topLevel == null)
            return;

        IStorageFile? file = await topLevel.StorageProvider.SaveFilePickerAsync(new FilePickerSaveOptions {
            Title = "CSV File name",
            DefaultExtension = "csv",
        });

        if(file != null) {

            var dc = new TableConverterViewModel(ViewModel.LatestParquetPath!, file.Path.LocalPath);

            var tcControl = new TableConverterView {
                HorizontalAlignment = Avalonia.Layout.HorizontalAlignment.Center,
                VerticalAlignment = Avalonia.Layout.VerticalAlignment.Center,
                Width = 800,
                DataContext = dc
            };

            dc.OnCloseRequested += () => {
                ClientArea.Children.Remove(tcControl);
            };

            ClientArea.Children.Add(tcControl);
        }
    }

    private void OpenUtil<T>(string title) where T : UserControl, new() {
        var wnd = new WindowContainer(title, new T());
        wnd.Width = 600;
        wnd.Height = 400;
        if(TopLevel.GetTopLevel(this) is Window topWindow) {
            wnd.ShowDialog(topWindow);
            //wnd.Show(topWindow);
            //wnd.Show();
        }
    }

    private void Util_Merger_Click(object? sender, Avalonia.Interactivity.RoutedEventArgs e) {
        OpenUtil<FileMergerUtil>("File merger");
    }
}

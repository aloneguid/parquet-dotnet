using System;
using System.IO;
using Avalonia.Controls;
using Parquet.Floor.ViewModels;
using Stowage;

namespace Parquet.Floor.Views;

public partial class FileExplorer : UserControl {
    public FileExplorer() {
        InitializeComponent();

        DataContext = new FileExplorerViewModel();
    }
}
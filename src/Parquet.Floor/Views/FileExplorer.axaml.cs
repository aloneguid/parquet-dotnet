using Avalonia.Controls;
using Parquet.Floor.ViewModels;

namespace Parquet.Floor.Views;

public partial class FileExplorer : UserControl {
    public FileExplorer() {
        InitializeComponent();

        DataContext = new FileExplorerViewModel();
    }
}
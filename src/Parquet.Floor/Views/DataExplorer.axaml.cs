using Avalonia.Controls;
using Parquet.Floor.ViewModels;

namespace Parquet.Floor.Views;

public partial class DataExplorer : UserControl {
    public DataExplorer() {
        InitializeComponent();

        DataContext = new FileExplorerViewModel();
    }
}
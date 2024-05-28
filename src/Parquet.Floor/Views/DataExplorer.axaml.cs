using Avalonia;
using Avalonia.Controls;
using Avalonia.Markup.Xaml;
using Parquet.Floor.ViewModels;

namespace Parquet.Floor.Views;

public partial class DataExplorer : UserControl
{
    public DataExplorer()
    {
        InitializeComponent();

        DataContext = new DataExplorerViewModel();
    }
}
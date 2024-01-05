using Avalonia.Controls;

namespace Parquet.Floor.Views;

public partial class MainWindow : Window {
    public MainWindow() {
        InitializeComponent();

        Title = $"Parquet Floor v{Parquet.Globals.Version}";
    }
}
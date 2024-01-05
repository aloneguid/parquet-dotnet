using Avalonia;
using Avalonia.Controls;
using Avalonia.Markup.Xaml;
using Parquet.Floor.ViewModels;

namespace Parquet.Floor.Views;

public partial class ErrorView : UserControl {
    public ErrorView() {
        InitializeComponent();
    }

    public MainViewModel? ViewModel => DataContext as MainViewModel;

    private void TextBlock_PointerPressed(object? sender, Avalonia.Input.PointerPressedEventArgs e) {
        if(ViewModel != null) {
            ViewModel.ShowErrorDetails = true;
        }
    }
}
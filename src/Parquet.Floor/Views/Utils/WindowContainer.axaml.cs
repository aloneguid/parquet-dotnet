using Avalonia;
using Avalonia.Controls;
using Avalonia.Markup.Xaml;

namespace Parquet.Floor.Views.Utils;

public partial class WindowContainer : Window {
    public WindowContainer() {
        InitializeComponent();
    }

    public WindowContainer(string title, UserControl control) : this() {
        Title = title;
        TextTitle.Text = title;
        ClientArea.Children.Add(control);
    }
}
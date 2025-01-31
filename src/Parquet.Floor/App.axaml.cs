using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Data.Core.Plugins;
using Avalonia.Markup.Xaml;
using Avalonia.Styling;
using Parquet.Floor.ViewModels;
using Parquet.Floor.Views;

namespace Parquet.Floor;

public partial class App : Application {
    public override void Initialize() {
        AvaloniaXamlLoader.Load(this);

        LoadThemeVariant();

        ActualThemeVariantChanged += (s, e) => {
            ThemeVariant variant = ActualThemeVariant;
            string? variantName = variant.Key?.ToString();
            Settings.Instance.ThemeVariant = variantName;
        };
    }

    private void LoadThemeVariant() {
        string? variant = Settings.Instance.ThemeVariant;
        if(variant != null) {
            if(variant == "Light") {
                RequestedThemeVariant = ThemeVariant.Light;
            } else if(variant == "Dark") {
                RequestedThemeVariant = ThemeVariant.Dark;
            } else {
                Settings.Instance.ThemeVariant = null;
            }
        }
    }

    public override void OnFrameworkInitializationCompleted() {
        // Line below is needed to remove Avalonia data validation.
        // Without this line you will get duplicate validations from both Avalonia and CT
#pragma warning disable IL2026 // Members annotated with 'RequiresUnreferencedCodeAttribute' require dynamic access otherwise can break functionality when trimming application code
        BindingPlugins.DataValidators.RemoveAt(0);
#pragma warning restore IL2026 // Members annotated with 'RequiresUnreferencedCodeAttribute' require dynamic access otherwise can break functionality when trimming application code

        var model = new MainViewModel();

        if(ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop) {
            desktop.MainWindow = new MainWindow() { DataContext = model };
        } else if(ApplicationLifetime is ISingleViewApplicationLifetime singleViewPlatform) {
            singleViewPlatform.MainView = new MainView() { DataContext = model };
        }

        base.OnFrameworkInitializationCompleted();
    }
}
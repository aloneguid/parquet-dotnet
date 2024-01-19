using System;
using Avalonia;
using Projektanker.Icons.Avalonia.FontAwesome;
using Projektanker.Icons.Avalonia;

namespace Parquet.Floor;

class Program {
    // Initialization code. Don't use any Avalonia, third-party APIs or any
    // SynchronizationContext-reliant code before AppMain is called: things aren't initialized
    // yet and stuff might break.
    [STAThread]
    public static void Main(string[] args) => BuildAvaloniaApp()
        .StartWithClassicDesktopLifetime(args);

    // Avalonia configuration, don't remove; also used by visual designer.
    public static AppBuilder BuildAvaloniaApp() {

        Tracker.Instance.Constants.Add("iid", Settings.Instance.InstanceId.ToString());
        Tracker.Instance.Constants.Add("os", Environment.OSVersion.Platform.ToString());
        Tracker.Instance.Constants.Add(Settings.TelemetryConstant, Settings.Instance.BasicTelemetryEnabled.ToString());

        try {
            Tracker.Instance.Track("start", force: true);
        }catch(Exception ex) {
            Console.WriteLine(ex);
        }

        IconProvider.Current
            .Register<FontAwesomeIconProvider>();

        return AppBuilder.Configure<App>()
            .UsePlatformDetect()
            //.WithInterFont()
            .LogToTrace();
    }

}
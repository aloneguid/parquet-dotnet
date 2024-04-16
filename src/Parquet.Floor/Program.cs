using System;
using Avalonia;
using Projektanker.Icons.Avalonia.FontAwesome;
using Projektanker.Icons.Avalonia;
using System.Threading.Tasks;

namespace Parquet.Floor;

class Program {
    // Initialization code. Don't use any Avalonia, third-party APIs or any
    // SynchronizationContext-reliant code before AppMain is called: things aren't initialized
    // yet and stuff might break.
    [STAThread]
    public static async Task Main(string[] args) {

        Tracker.Instance = new Tracker("floor", Globals.Version);
        Tracker.Instance.Constants.Add("iid", Settings.Instance.InstanceId.ToString());
        Tracker.Instance.Constants.Add("os", Environment.OSVersion.Platform.ToString());

        try {
            BuildAvaloniaApp().StartWithClassicDesktopLifetime(args);
        } catch {
            await Tracker.Instance.FlushAsync();
        }
    }

    // Avalonia configuration, don't remove; also used by visual designer.
    public static AppBuilder BuildAvaloniaApp() {
        try {
            Tracker.Instance.Track("start");
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
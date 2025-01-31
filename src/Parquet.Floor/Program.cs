using System;
using Avalonia;
using Projektanker.Icons.Avalonia.FontAwesome;
using Projektanker.Icons.Avalonia;
using System.Threading.Tasks;
using System.IO.Pipes;
using System.IO;
using System.Threading;
using CommunityToolkit.Mvvm.Messaging;
using Parquet.Floor.Messages;
using Parquet.Floor.Views;
using System.Runtime.InteropServices;

namespace Parquet.Floor;

class Program {

    private const string MutexName = "Parquet.Floor.SingleInstance";
    private const string PipeName = "Parquet.Floor.Pipe";
    private static readonly CancellationTokenSource _cts = new CancellationTokenSource();

    [STAThread]
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
    public static async Task Main(string[] args) {
        using(var mutex = new Mutex(true, MutexName, out bool isNewInstance)) {
            if(!isNewInstance) {
                // If another instance is already running, send data to it and exit
                using(var client = new NamedPipeClientStream(PipeName)) {
                    client.Connect();
                    using(var writer = new StreamWriter(client)) {
                        writer.WriteLine(string.Join(" ", args));
                        writer.Flush();
                    }
                }
                return;
            }

            // Start a new thread to listen for incoming data
            ListenForDataAsync().Forget();

            Tracker.Instance = new Tracker("floor", Globals.Version);
            Tracker.Instance.Constants.Add("iid", Settings.Instance.InstanceId.ToString());
            Tracker.Instance.Constants.Add("os", RuntimeInformation.OSDescription);

            try {
                AppBuilder app = BuildAvaloniaApp();
                app.StartWithClassicDesktopLifetime(args);
            } catch {
                // report error
            } finally {
                Tracker.Instance.Track("stop", force: true);
            }
        }
    }
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously

    private static async Task ListenForDataAsync() {

        while(!_cts.IsCancellationRequested) {
            using var server = new NamedPipeServerStream(PipeName);
            await server.WaitForConnectionAsync(_cts.Token);
            using var reader = new StreamReader(server);
            string? data = reader.ReadLine();

            // open file
            if(!string.IsNullOrEmpty(data)) {
                WeakReferenceMessenger.Default.Send(new FileOpenMessage(data));
            }
        }
    }

    // Avalonia configuration, don't remove; also used by visual designer.
    public static AppBuilder BuildAvaloniaApp() {
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
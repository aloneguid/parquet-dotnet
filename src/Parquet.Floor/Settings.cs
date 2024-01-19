using System;
using System.IO;
using Config.Net;

namespace Parquet.Floor {

    public interface ISettings {
        Guid InstanceId { get; set; }

        bool BasicTelemetryEnabled { get; set; }

        bool TelemetryDecisionMade { get; set; }

        string? ThemeVariant { get; set; }
    }

    static class Settings {

        public const string TelemetryConstant = "tel";
        public static ISettings Instance { get; private set; }

        static Settings() {
            string path = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "Parquet.Floor",
                "settings.ini");

            Instance = new ConfigurationBuilder<ISettings>()
                .UseIniFile(path)
                .Build();

            if(Instance.InstanceId == Guid.Empty) {
                Instance.InstanceId = Guid.NewGuid();
            }
        }
    }
}

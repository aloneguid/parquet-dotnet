using System.Runtime.InteropServices;
using Xunit;

namespace Parquet.Test.Xunit {
    public class SkipOnMacAttribute : FactAttribute {
        public SkipOnMacAttribute() {
            if(RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) {
                Skip = "Skip on Mac";
            }
        }
    }
}

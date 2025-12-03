using System.Runtime.InteropServices;
using Xunit;

namespace Parquet.Test.Xunit {
    public class NonMacOSFactAttribute : FactAttribute {
        public NonMacOSFactAttribute() {
            if(RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) {
                Skip = "Skip on Mac";
            }
        }
    }
}

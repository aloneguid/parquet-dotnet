using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace Parquet.Test.Integration {
    public class IntegrationBase : TestBase {
        private readonly string _toolsPath;
        private readonly string _toolsJarPath;
        private readonly string _javaExecName;

        public IntegrationBase() {
            _toolsPath = Path.GetFullPath(Path.Combine("..", "..", "..", "..", "..", "tools"));
            _toolsJarPath = Path.Combine(_toolsPath, "parquet-tools-1.9.0.jar");

            _javaExecName = Environment.OSVersion.Platform == PlatformID.Win32NT
               ? "java.exe"
               : "java";
        }

        private string? ExecJavaAndGetOutput(string arguments) {
            var psi = new ProcessStartInfo {
                FileName = _javaExecName,
                Arguments = arguments,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            var proc = new Process { StartInfo = psi };

            if(!proc.Start())
                return null;

            var so = new StringBuilder();
            var se = new StringBuilder();

            while(!proc.StandardOutput.EndOfStream) {
                string? line = proc.StandardOutput.ReadLine();
                if(line != null) {
                    so.AppendLine(line);
                }
            }

            while(!proc.StandardError.EndOfStream) {
                string? line = proc.StandardError.ReadLine();
                if(line != null) {
                    se.AppendLine(line);
                }
            }

            proc.WaitForExit();

            if(proc.ExitCode != 0) {
                throw new Exception("process existed with code " + proc.ExitCode + ", error: " + se.ToString());
            }

            return so.ToString().Trim();
        }

        protected string? ExecMrCat(string testFileName) {
            return ExecJavaAndGetOutput($"-jar \"{_toolsJarPath}\" cat -j \"{testFileName}\"");
        }
    }
}

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CommunityToolkit.Mvvm.ComponentModel;
using Parquet.Utils;

namespace Parquet.Floor.ViewModels.Utils {
    public partial class FileMergerViewModel : ViewModelBase {
        public ObservableCollection<FileInfo> SourceFiles { get; } = new();

        [ObservableProperty]
        private string? _sourceFilesDisplay;

        [ObservableProperty]
        private FileInfo? _targetFile;

        [ObservableProperty]
        private bool _canStart;

        [ObservableProperty]
        private bool _mergeColumns;

        [ObservableProperty]
        private string? _error;

        [ObservableProperty]
        private string? _result;

        [ObservableProperty]
        private bool _isRunning;

        private void CheckCanStart() {
            CanStart = SourceFiles.Count > 0 && TargetFile != null && !IsRunning;
        }

        public void SetInput(string inputFolderPath) {
            IEnumerable<string> files = new FileMerger(new DirectoryInfo(inputFolderPath)).InputFiles.Select(f => f.FullName);
            SetInput(files);
        }

        public void SetInput(IEnumerable<string> inputFilePaths) {
            SourceFiles.Clear();
            foreach(FileInfo? file in inputFilePaths.Select(f => new FileInfo(f))) {
                SourceFiles.Add(file);
            }
            SourceFilesDisplay = 
                $"{SourceFiles.Count}: " +
                string.Join(", ", SourceFiles.Select(f => f.Name));
            CheckCanStart();
        }

        public void SetOutput(string targetFilePath) {
            TargetFile = new FileInfo(targetFilePath);
            CheckCanStart();
        }

        public async Task Run() {
            using var merger = new FileMerger(SourceFiles);

            using FileStream destStream = System.IO.File.Create(TargetFile!.FullName);

            try {
                IsRunning = true;
                CheckCanStart();
                if(MergeColumns) {
                    await merger.MergeRowGroups(destStream);
                } else {
                    await merger.MergeFilesAsync(destStream);
                }

                Result = $"{merger.InputStreams.Count} file(s) merged.";
            } catch(Exception ex) {
                Error = ex.Message;
            } finally {
                IsRunning = false;
                CheckCanStart();
            }
        }
    }
}

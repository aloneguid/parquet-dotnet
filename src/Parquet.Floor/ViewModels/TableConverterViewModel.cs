using System;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using Parquet.Floor.Controllers;

namespace Parquet.Floor.ViewModels {
    partial class TableConverterViewModel : ViewModelBase {
        private readonly string _parquetFilePath;
        private readonly string _csvFilePath;

        [ObservableProperty]
        private string? _progressText;

        [ObservableProperty]
        private bool _isIndeterminate;

        [ObservableProperty]
        private int _progressPercentage;

        [ObservableProperty]
        private bool _isFinished;

        [ObservableProperty]
        private string? _errorMessage;

        [ObservableProperty]
        private string? _errorDetails;

        public event Action? OnCloseRequested;

        public TableConverterViewModel() {
            _parquetFilePath = "";
            _csvFilePath = "";

#if DEBUG
            if(Design.IsDesignMode) {
                ProgressText = "Reading source file...";
                IsIndeterminate = false;
                ProgressPercentage = 30;
            }
#endif
        }

        public void CloseThis() {
            OnCloseRequested?.Invoke();
        }

        public TableConverterViewModel(string parquetFilePath, string csvFilePath) {
            _parquetFilePath = parquetFilePath;
            _csvFilePath = csvFilePath;

            RunAsync().Forget();
        }

        private async Task RunAsync() {
            Dispatcher.UIThread.Invoke(() => {
                IsIndeterminate = true;
                ProgressText = "loading parquet file...";
                ProgressPercentage = 0;
            });

            try {

                using(System.IO.FileStream stream = System.IO.File.OpenRead(_parquetFilePath)) {
                    using(var converter = new ParquetToCsvConverter(stream, _csvFilePath)) {
                        converter.OnFileOpened += (long rowCount) => {
                            Dispatcher.UIThread.Invoke(() => {
                                ProgressText = "converting...";
                                IsIndeterminate = false;
                                ProgressPercentage = 0;
                            });
                        };

                        converter.OnRowConverted += (long rowNumber, long totalRows) => {
                            Dispatcher.UIThread.Invoke(() => {
                                ProgressPercentage = (int)((double)rowNumber / totalRows * 100);
                            });
                        };

                        await converter.ConvertAsync();
                    }
                }

                Dispatcher.UIThread.Invoke(() => {
                    ProgressText = "done";
                });

            } catch(Exception ex) {
                Dispatcher.UIThread.Invoke(() => {
                    ProgressText = "failed";
                    ErrorMessage = ex.Message;
                    ErrorDetails = ex.ToString();
                });
            } finally {
                Dispatcher.UIThread.Invoke(() => {
                    IsFinished = true;
                });
            }
        }
    }
}

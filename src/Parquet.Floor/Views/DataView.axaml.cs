using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Avalonia.Controls;
using Avalonia.Data;
using Avalonia.Markup.Xaml.MarkupExtensions.CompiledBindings;
using Avalonia.Threading;
using Parquet.Floor.ViewModels;
using Parquet.Floor.Views.Templates;
using Parquet.Schema;

namespace Parquet.Floor.Views {
    public partial class DataView : UserControl, IDataViewGridView {

        public DataView() {
            InitializeComponent();
        }

        private DataViewModel? Model => DataContext as DataViewModel;

        protected override void OnDataContextChanged(EventArgs e) {
            if(Model != null) {
                Model.PropertyChanged += ViewModel_PropertyChanged;
                Model.DataGridView = this;
            }
        }

        private DataGridColumn CreateColumn(Field f) {
            return new DataGridTemplateColumn {
                Header = f.Name,
                HeaderTemplate = new DataViewHeaderTemplate(f),
                CellTemplate = new DataViewCellTemplate(f)
            };
        }

        private IEnumerable<DataGridColumn>? BuildColumns(ParquetSchema schema) {
            return schema == null
                ? null
                : schema.Fields.Select(CreateColumn).ToList();
        }

        private void ViewModel_PropertyChanged(object? sender, PropertyChangedEventArgs e) {
            Dispatcher.UIThread.Invoke(() => {

                if(Model?.File?.Schema == null)
                    return;

                if(e.PropertyName == nameof(DataViewModel.File)) {
                    // copy the columns over, as DataGrid.Columns does not support binding

                    grid.Columns.Clear();
                    IEnumerable<DataGridColumn>? columns = BuildColumns(Model.File.Schema);

                    if(columns != null) {
                        foreach(DataGridColumn c in columns) {
                            grid.Columns.Add(c);
                        }
                    }
                }
            });
        }

        private void DataGrid_CellPointerPressed(object? sender, Avalonia.Controls.DataGridCellPointerPressedEventArgs e) {
            if(e is null)
                throw new ArgumentNullException(nameof(e));
            grid.SelectedItem = null;
        }

        public string? GetFormattedSelectedRowText(bool copyToClipboard) {
            if(grid.SelectedItem is Dictionary<string, object> row) {
                string text = string.Join(Environment.NewLine, row.Select(kvp => $"{kvp.Key}: {kvp.Value}"));

                if(copyToClipboard) {
                    TopLevel.GetTopLevel(this)?.Clipboard?.SetTextAsync(text);
                }

                return text;
            }

            return null;
        }
    }
}

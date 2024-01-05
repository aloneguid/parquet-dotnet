using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Avalonia.Controls;
using Avalonia.Data;
using Avalonia.Threading;
using Parquet.Floor.ViewModels;
using Parquet.Schema;

namespace Parquet.Floor.Views {
    public partial class DataView : UserControl {
        public DataView() {
            InitializeComponent();
        }

        public DataViewModel? ViewModel => DataContext as DataViewModel;


        protected override void OnDataContextChanged(EventArgs e) {

            if(ViewModel != null) { 
                ViewModel.PropertyChanged += ViewModel_PropertyChanged;
            }

            base.OnDataContextChanged(e);
        }

        private IEnumerable<DataGridColumn>? BuildColumns(ParquetSchema schema) {
            // build columns

            return schema == null
                ? null
                : schema.Fields.Select(f => new DataGridTemplateColumn {
                    Header = f.Name,
                    CellTemplate = new DataViewCellTemplate(f)
                }).Cast<DataGridColumn>().ToList();
        }

        private void ViewModel_PropertyChanged(object? sender, PropertyChangedEventArgs e) {
            Dispatcher.UIThread.Invoke(() => {

                if(ViewModel == null || ViewModel.File?.Schema == null)
                    return;

                if(e.PropertyName == nameof(DataViewModel.File)) {
                    // copy the columns over, as DataGrid.Columns does not support binding

                    grid.Columns.Clear();
                    IEnumerable<DataGridColumn>? columns = BuildColumns(ViewModel.File.Schema);

                    if(columns != null) {
                        foreach(DataGridColumn c in columns) {
                            grid.Columns.Add(c);
                        }
                    }
                } 
            });
        }

    }
}

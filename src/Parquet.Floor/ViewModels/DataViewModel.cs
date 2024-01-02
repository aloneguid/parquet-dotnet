using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using CommunityToolkit.Mvvm.ComponentModel;
using Parquet.Serialization;
using Parquet.Schema;

namespace Parquet.Floor.ViewModels;

public class CellModel {
    public CellModel(Parquet.Schema.Field f, Dictionary<string, object> row) {

    }
}

public partial class DataViewModel : ViewModelBase {

    [ObservableProperty]
    private ParquetSchema? _schema;

    [ObservableProperty]
    private IList<Dictionary<string, object>> _data;

    public void InitReader(Stream fileStream) {
        Task.Run(async () => {
            try {
                ParquetSerializer.UntypedResult fd = await ParquetSerializer.DeserializeAsync(fileStream);
                Schema = fd.Schema;
                Data = fd.Data;

            } catch(Exception ex) {
                Console.WriteLine(ex);
            }

        });
    }
}

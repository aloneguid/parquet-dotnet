using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Controls.Templates;
using Parquet.Schema;
using ZstdSharp.Unsafe;

namespace Parquet.Floor.Views {
    internal class DataViewCellTemplate : IDataTemplate {
        private readonly Field _field;

        public DataViewCellTemplate(Field field) {
            _field = field;
        }

        public Control? Build(object? param) {

            if(_field.SchemaType == SchemaType.Data) {

                string? sv = null;

                if(param is Dictionary<string, object> row) {
                    if(row.TryGetValue(_field.Name, out object? value)) {
                        if(value != null) {
                            sv = value.ToString();
                        }
                    }
                }

                bool isNull = sv == null;
                var tx = new TextBlock() {
                    Text = sv ?? "null"
                };
                tx.Classes.Add(isNull ? "data-null" : "data-cell");
                return tx;
            }

            return new Control();
        }

        public bool Match(object? data) => true;
    }
}

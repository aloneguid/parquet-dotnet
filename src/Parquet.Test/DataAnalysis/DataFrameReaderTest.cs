using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Analysis;
using Parquet.Encodings;
using Parquet.Schema;
using Xunit;
using System.Linq;
using Parquet.Data;
using NetBox.Generator;

namespace Parquet.Test.DataAnalysis {
    public class DataFrameReaderTest {

        private Array CreateData(Type t) {
            switch (t) {
                case Type intType when intType == typeof(int):
                    return new int[] { 1, 2, 3 };
                case Type boolType when boolType == typeof(bool):
                    return new[] { true, false, true };
                default:
                    throw new NotImplementedException(t.ToString());
            }
        }

        [Fact]
        public async Task Read_all_types() {
            using var ms = new MemoryStream();

            Type[] types = SchemaEncoder.SupportedTypes.Take(2).ToArray();

            // make schema
            var schema = new ParquetSchema(types.Select(t => new DataField(t.Name, t)).ToList());

            // make data
            using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) {
                using ParquetRowGroupWriter rgw = writer.CreateRowGroup();

                foreach(Type t in types) {
                    var dc = new DataColumn(
                        schema.DataFields.First(f => f.Name == t.Name), CreateData(t));
                    await rgw.WriteColumnAsync(dc);
                }
            }

            // read as DataFrame
            ms.Position = 0;
            DataFrame df = await ms.ReadAsDataFrameAsync();
        }
    }
}

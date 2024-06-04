using System;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Schema {
    public class DataColumnTest {

        [Fact]
        public void Construct_wrong_data_type() {
            var schema = new ParquetSchema(new DataField<int>("id"));

            Assert.Throws<ArgumentException>(() => new DataColumn(schema.GetDataFields()[0], new float[] { 1, 2, 3 }));
        }

        [Fact]
        public void Construct_no_data_no_definiton_or_repetition_check() {
            var schema = new ParquetSchema(new ListField("ids", new DataField<int>("element")));

            new DataColumn(schema.GetDataFields()[0], new int[0], null, null);
        }

        [Fact]
        public void Contatenate_data() {
            DataField df = new ParquetSchema(new DataField<int?>("id")).DataFields[0];
            var dc1 = new DataColumn(df, new int?[] { 1, null, 2 });
            var dc2 = new DataColumn(df, new int?[] { null, null, 3 });

            DataColumn merged = DataColumn.Concat(new[] { dc1, dc2 });
            Assert.Equal(new int?[] { 1, null, 2, null, null, 3 }, merged.Data);
            Assert.Equal(new int[] { 1, 2, 3 }, merged.DefinedData);
            Assert.Equal(new int[] { 1, 0, 1, 0, 0, 1 }, merged.DefinitionLevels);
            Assert.Null(merged.RepetitionLevels);
        }
    }
}

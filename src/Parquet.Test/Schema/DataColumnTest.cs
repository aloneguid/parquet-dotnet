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
    }
}

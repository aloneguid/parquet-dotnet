using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.Data;
using Parquet.Rows;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Rows {

    public class DataColumnsToRowsConverterTest : TestBase {

        [Fact]
        public void Convert_should_process_deeply_nested_structs_with_no_null_values() {
            // arrange
            // create a complex nested schema
            /*
             * { id, obj: { metadata, arrray: [ {prop1, prop2}, {prop1, prop2} ] } }
             */
            var prop1 = new DataField("prop1", typeof(string), isNullable: true);
            var prop2 = new DataField("prop2", typeof(DateTime?), isNullable: true);
            var array = new ListField("array", new StructField("item", new Field[] { prop1, prop2 }));
            var metadata = new DataField("metadata", typeof(int), isNullable: false);
            var obj = new StructField("obj", new Field[] { metadata, array });
            var idField = new DataField("id", typeof(string), isNullable: true);

            var schema = new ParquetSchema(new Field[] { idField, obj });

            DateTime now = DateTime.Now;

            DataColumn[] columns = new[] {
                new DataColumn(idField, new string[] { "id-1" }),
                new DataColumn(metadata, new int[] { 1 }),
                new DataColumn(prop1, new string[] { "prop1-value" }, new int[] { 0,1 }),
                new DataColumn(prop2, new DateTime?[] { now }, new int[] { 0, 1 }),
            };

            long totalRowRount = 1;
            var sut = new DataColumnsToRowsConverter(schema, columns, totalRowRount);

            // act
            IReadOnlyCollection<Row> rows = sut.Convert();

            // assert
            Assert.Single(rows);

            Row row = rows.ElementAt(0);

            string actualId = row.Values[0] as string;
            Assert.Equal("id-1", actualId);

            dynamic actualObj = row.Values[1] as dynamic;
            int actualMetadata = actualObj.Values[0];

            Assert.Equal(1, actualMetadata);

            dynamic actualArray = actualObj.Values[1] as dynamic;
            Assert.Equal("prop1-value", actualArray[0].Values[0]);
            Assert.Equal(now, actualArray[0].Values[1]);
        }

        [Fact]
        public void Convert_should_process_deeply_nested_structs_with_nulls_deeply_nested() {
            // arrange
            // create a complex nested schema
            /*
             * { id, obj: { metadata, arrray: [ {prop1, prop2}, {prop1, prop2} ] } }
             */
            var prop1 = new DataField("prop1", typeof(string), isNullable: true);
            var prop2 = new DataField("prop2", typeof(DateTime?), isNullable: true);
            var array = new ListField("array", new StructField("item", new Field[] { prop1, prop2 }));
            var metadata = new DataField("metadata", typeof(int), isNullable: false);
            var obj = new StructField("obj", new Field[] { metadata, array });
            var idField = new DataField("id", typeof(string), isNullable: true);

            var schema = new ParquetSchema(new Field[] { idField, obj });

            DateTime now = DateTime.Now;

            DataColumn[] columns = new[] {
                new DataColumn(idField, new string[] { "id-1" }),
                new DataColumn(metadata, new int[] { 1 }),
                new DataColumn(prop1, new string[] { null }, new int[] { 0,1 }),
                new DataColumn(prop2, new DateTime?[] { now }, new int[] { 0, 1 }),
            };

            long totalRowRount = 1;
            var sut = new DataColumnsToRowsConverter(schema, columns, totalRowRount);

            // act
            IReadOnlyCollection<Row> rows = sut.Convert();

            // assert
            Assert.Single(rows);

            Row row = rows.ElementAt(0);

            string actualId = row.Values[0] as string;
            Assert.Equal("id-1", actualId);

            dynamic actualObj = row.Values[1] as dynamic;
            int actualMetadata = actualObj.Values[0];

            Assert.Equal(1, actualMetadata);

            dynamic actualArray = actualObj.Values[1] as dynamic;
            Assert.Equal(1, actualArray.Length);
            Assert.Equal(now, actualArray[0].Values[0]);
        }
    }
}
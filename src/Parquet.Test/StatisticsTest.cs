using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test {
    public class StatisticsTest : TestBase {
        class TestDesc {
            public Type? Type { get; set; }

            public Array? Data { get; set; }

            public long? DistinctCount { get; set; }

            public long NullCount { get; set; }

            public object? Min { get; set; }

            public object? Max { get; set; }
        }


        private static readonly Dictionary<string, TestDesc> NameToTest = new Dictionary<string, TestDesc> {
            ["int"] =
              new TestDesc {
                  Type = typeof(int),
                  Data = new int[] { 4, 2, 1, 3, 5, 1, 4 },
                  NullCount = 0,
                  DistinctCount = 5,
                  Min = 1,
                  Max = 5
              },
            ["int?"] =
              new TestDesc {
                  Type = typeof(int?),
                  Data = new int?[] { 4, 2, 1, 3, 1, null, 4 },
                  DistinctCount = 4,
                  NullCount = 1,
                  Min = 1,
                  Max = 4
              },
            ["string"] =
              new TestDesc {
                  Type = typeof(string),
                  Data = new string[] { "one", "two", "one" },
                  NullCount = 0,
                  DistinctCount = 2,
                  Min = "one",
                  Max = "two"
              },
            ["float"] =
              new TestDesc {
                  Type = typeof(float),
                  Data = new float[] { 1.23f, 2.1f, 0.5f, 0.5f },
                  DistinctCount = 3,
                  NullCount = 0,
                  Min = 0.5f,
                  Max = 2.1f
              },
            ["double"] =
              new TestDesc {
                  Type = typeof(double),
                  Data = new double[] { 1.23D, 2.1D, 0.5D, 0.5D },
                  DistinctCount = 3,
                  NullCount = 0,
                  Min = 0.5D,
                  Max = 2.1D
              },
            ["dateTime"] =
                new TestDesc {
                    Type = typeof(DateTime),
                    Data = new DateTime[]
                    {
                        new DateTime(2019, 12, 16, 0, 0, 0, DateTimeKind.Utc),
                        new DateTime(2019, 12, 16, 0, 0, 0, DateTimeKind.Utc),
                        new DateTime(2019, 12, 15, 0, 0, 0, DateTimeKind.Utc),
                        new DateTime(2019, 12, 17, 0, 0, 0, DateTimeKind.Utc)
                    },
                    DistinctCount = 3,
                    NullCount = 0,
                    Min = new DateTime(2019, 12, 15, 0, 0, 0, DateTimeKind.Utc),
                    Max = new DateTime(2019, 12, 17, 0, 0, 0, DateTimeKind.Utc)
                },
            ["dateTime unknown"] =
                new TestDesc {
                    Type = typeof(DateTime),
                    Data = new DateTime[]
                    {
                        new DateTime(2019, 12, 16),
                        new DateTime(2019, 12, 16),
                        new DateTime(2019, 12, 15),
                        new DateTime(2019, 12, 17)
                    },
                    DistinctCount = 3,
                    NullCount = 0,
                    Min = new DateTime(2019, 12, 15, 0, 0, 0, DateTimeKind.Utc),
                    Max = new DateTime(2019, 12, 17, 0, 0, 0, DateTimeKind.Utc)
                },
            ["dateTime local"] =
                new TestDesc {
                    Type = typeof(DateTime),
                    Data = new DateTime[]
                    {
                        new DateTime(2019, 12, 16, 0, 0, 0, DateTimeKind.Local),
                        new DateTime(2019, 12, 16, 0, 0, 0, DateTimeKind.Local),
                        new DateTime(2019, 12, 15, 0, 0, 0, DateTimeKind.Local),
                        new DateTime(2019, 12, 17, 0, 0, 0, DateTimeKind.Local)
                    },
                    DistinctCount = 3,
                    NullCount = 0,
                    Min = new DateTime(2019, 12, 15, 0, 0, 0, DateTimeKind.Local).ToUniversalTime(),
                    Max = new DateTime(2019, 12, 17, 0, 0, 0, DateTimeKind.Local).ToUniversalTime()
                }
        };

        [Theory]
        [InlineData("int")]
        [InlineData("int?")]
        [InlineData("string")]
        [InlineData("float")]
        [InlineData("double")]
        [InlineData("dateTime")]
        [InlineData("dateTime unknown")]
        [InlineData("dateTime local")]
        public async Task Distinct_stat_for_basic_data_types(string name) {
            TestDesc? test = NameToTest[name];

            var schema = new ParquetSchema(new DataField("id", test!.Type!));
            DataField id = schema.GetDataFields()[0];

            DataColumn? rc = await WriteReadSingleColumn(new DataColumn(id, test!.Data!));

            Assert.Equal(test.Data!.Length, rc!.CalculateRowCount());
            //Assert.Equal(test.DistinctCount, rc.Statistics.DistinctCount);
            Assert.Equal(test.NullCount, rc.Statistics.NullCount);
            Assert.Equal(test.Min, rc.Statistics.MinValue);
            Assert.Equal(test.Max, rc.Statistics.MaxValue);
        }
    }
}
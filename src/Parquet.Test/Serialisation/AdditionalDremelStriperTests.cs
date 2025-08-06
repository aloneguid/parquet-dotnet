using System.Collections.Generic;
using Parquet.Serialization;
using Parquet.Serialization.Dremel;
using Parquet.Test.Serialisation.Paper;
using Xunit;

namespace Parquet.Test.Serialisation {
    public class TestRow {
        public List<int>? Numbers { get; set; } = [];
        public List<string?>? Words { get; set; } = [];   
    }

    /// <summary>
    /// Additional tests beyond the example given in the google paper, but needed for completeness.
    /// </summary>
    public class AdditionalDremelStriperTests {
        private readonly Striper<TestRow> _striper;

        public AdditionalDremelStriperTests() {
            _striper = new Striper<TestRow>(typeof(TestRow).GetParquetSchema(false));
        }

        public static List<TestRow> NullEmptyListTestCases = [
            new TestRow { Numbers = [123], Words = ["abc"] },
            new TestRow { Numbers = [], Words = [] },
            new TestRow { Numbers = [], Words = [null] },
            new TestRow { Numbers = null, Words = null },
        ];

        [Fact]
        public void Empty_Null_Lists_Nullable_Item() {
            FieldStriper<TestRow> striper = _striper.FieldStripers[1];
            ShreddedColumn col = striper.Stripe(striper.Field, NullEmptyListTestCases);
            Assert.Equal(new string[] {"abc"}, col.Data);
            Assert.Equal(new int[] { 0, 0, 0, 0 }, col.RepetitionLevels!);
            Assert.Equal(new int[] { 3, 1, 2, 0}, col.DefinitionLevels!);
        }

        [Fact]
        public void Empty_Null_Lists_NonNullable_Item() {
            FieldStriper<TestRow> striper = _striper.FieldStripers[0];
            ShreddedColumn col = striper.Stripe(striper.Field, NullEmptyListTestCases);
            Assert.Equal(new int[] {123}, col.Data);
            Assert.Equal(new int[] { 0, 0, 0, 0 }, col.RepetitionLevels!);
            Assert.Equal(new int[] { 2, 1, 1, 0}, col.DefinitionLevels!);
        }
    }
}

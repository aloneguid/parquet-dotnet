using System;
using System.IO;
using System.Numerics;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.File.Values.Primitives;
using Parquet.Rows;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Types {
    public class ListOfStructuresTest {
        [Theory]
        [MemberData(nameof(NullableDataFields))]
        public async Task List_of_structures_with_nullable_fields_sets_correct_definition_levels(DataField nullableDataField) {

            var schema = new ParquetSchema(
                new DataField<int>("data_field"),
                new ListField("list_field",
                    new StructField("struct_field",
                        new DataField<int>("list_struct_field_not_nullable"),
                        nullableDataField
                    )
                )
            );

            var table = new Table(schema)
            {
                {
                    1,
                    new[]
                    {
                        new Row
                        (
                            2,
                            null
                        )
                    }
                }
            };

            using var memoryStream = new MemoryStream();
            await table.WriteAsync(memoryStream);
            using ParquetReader reader = await ParquetReader.CreateAsync(memoryStream);
            using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0);
            DataField[] dataFields = reader.Schema.GetDataFields();
            DataColumn dataColumn = await rowGroupReader.ReadColumnAsync(nullableDataField);
            Assert.Equal(new int[] { 3 }, dataColumn.DefinitionLevels);
        }

        public static TheoryData<DataField> NullableDataFields => new() {
            new DataField<bool?>("bool"),
            new DataField<byte?>("byte"),
            new DataField<sbyte?>("sbyte"),
            new DataField<short?>("short"),
            new DataField<ushort?>("ushort"),
            new DataField<int?>("int"),
            new DataField<uint?>("uint"),
            new DataField<long?>("long"),
            new DataField<ulong?>("ulong"),
            //new DataField<BigInteger?>("BigInteger"),
            new DataField<float?>("float"),
            new DataField<double?>("double"),
            new DataField<decimal?>("decimal"),
            new DataField<DateTime?>("DateTime"),
            new DataField<TimeSpan?>("TimeSpan"),
            new DataField<Interval?>("Interval"),
            new DataField<string>("string"),
            new DataField<byte[][]>("byteArrayOfArrays"),
            new DataField<Guid?>("Guid"),
            new DataField<DateOnly?>("DateOnly"),
            new DataField<TimeOnly?>("TimeOnly"),
        };
    }
}
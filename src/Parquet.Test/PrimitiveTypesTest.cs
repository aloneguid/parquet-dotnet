using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test {
    public class PrimitiveTypesTest : TestBase {
        [Theory]
        [InlineData(100)]
        [InlineData(1000)]
        public async Task Write_loads_of_booleans_all_true(int count) {
            var id = new DataField<bool>("enabled");
            var schema = new ParquetSchema(id);

            bool[] data = new bool[count];
            //generate data
            for(int i = 0; i < count; i++) {
                data[i] = true;
            }

            DataColumn? read = await WriteReadSingleColumn(new DataColumn(id, data));

            for(int i = 0; i < count; i++) {
                Assert.True((bool)read!.Data.GetValue(i)!, $"got FALSE at position {i}");
            }

        }

        [Theory]
        [InlineData(100)]
        public async Task Write_bunch_of_uints(uint count) {
            var schema = new ParquetSchema(new DataField<uint>("uint"));

            uint[] data = new uint[count];
            for(uint i = 0; i < count; i++) {
                data[i] = uint.MaxValue - i;
            }

            DataColumn? read = await WriteReadSingleColumn(new DataColumn(schema.GetDataFields()[0], data));
            for(uint i = 0; i < count; i++) {
                uint result = (uint)read!.Data.GetValue(i)!;
                Assert.Equal(uint.MaxValue - i, result);
            }
        }

        [Theory]
        [InlineData(100)]
        public async Task Write_bunch_of_ulongs(ulong count) {
            var schema = new ParquetSchema(new DataField<ulong>("longs"));

            ulong[] data = new ulong[count];
            for(uint i = 0; i < count; i++) {
                data[i] = ulong.MaxValue - i;
            }

            DataColumn? read = await WriteReadSingleColumn(new DataColumn(schema.GetDataFields()[0], data));
            for(uint i = 0; i < count; i++) {
                ulong result = (ulong)read!.Data.GetValue(i)!;
                Assert.Equal(ulong.MaxValue - i, result);
            }
        }
    }
}
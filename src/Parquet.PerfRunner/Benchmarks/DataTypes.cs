using Parquet.Data;
using Parquet.Schema;

namespace Parquet.PerfRunner.Benchmarks {
    internal class DataTypes {

        private const int DataSize = 1000000;
        //private Parquet.Data.DataColumn _ints;

        private readonly ParquetSchema _nullableIntsSchema = new ParquetSchema(new DataField<int?>("i"));
        private readonly Parquet.Data.DataColumn _nullableInts;

        private readonly ParquetSchema _nullableDecimalsSchema = new ParquetSchema(new DataField<decimal?>("i"));
        private readonly Parquet.Data.DataColumn _nullableDecimals;


        //private DataColumn _randomStrings;
        //private DataColumn _repeatingStrings;

        private static readonly Random random = new Random();

        public static string RandomString(int length) {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        public DataTypes() {
            //_ints = new DataColumn(new DataField<int>("c"), Enumerable.Range(0, DataSize).ToArray());

            _nullableInts = new DataColumn(_nullableIntsSchema.DataFields[0],
                Enumerable
                    .Range(0, DataSize)
                    .Select(i => i % 4 == 0 ? (int?)null : i)
                    .ToArray());

            _nullableDecimals = new DataColumn(_nullableDecimalsSchema.DataFields[0],
                Enumerable
                    .Range(0, DataSize)
                    .Select(i => i % 4 == 0 ? (decimal?)null : (decimal)i)
                    .ToArray());


            //_randomStrings = new DataColumn(new DataField<string>("c"),
            //    Enumerable.Range(0, DataSize)
            //        .Select(i => RandomString(50))
            //        .ToArray());

            //_repeatingStrings = new DataColumn(new DataField<string>("c"),
            //    Enumerable.Range(0, DataSize)
            //        .Select(i => i < DataSize / 2 ? "first half" : "second half")
            //        .ToArray());
        }

        private async Task Run(DataColumn c) {
            using var ms = new MemoryStream();

            using(ParquetWriter writer = await ParquetWriter.CreateAsync(new ParquetSchema(c.Field), ms)) {
                writer.CompressionMethod = CompressionMethod.None;
                // create a new row group in the file
                using(ParquetRowGroupWriter groupWriter = writer.CreateRowGroup()) {
                    await groupWriter.WriteColumnAsync(c);
                }
            }

            ms.Position = 0;
            using(ParquetReader reader = await ParquetReader.CreateAsync(ms)) {
                using(ParquetRowGroupReader rg = reader.OpenRowGroupReader(0)) {
                    await rg.ReadColumnAsync(c.Field);
                }
            }
        }

        public Task NullableInts() {
            return Run(_nullableInts);
        }

        public Task NullableDecimals() {
            return Run(_nullableDecimals);
        }

        //public Task RandomStrings() {
        //    return Run(_randomStrings);
        //}
    }
}

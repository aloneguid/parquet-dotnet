using Parquet.Data;
using Parquet.Schema;

namespace Parquet.PerfRunner.Benchmarks {
    public abstract class BenchmarkBase {

        public const int DataSize = 1_000_000;

        public static Array CreateTestData(Type t) {
            Array ar;
            if(t == typeof(int)) {
                ar = Enumerable.Range(0, DataSize).ToArray();
            } else if(t == typeof(int?)) {
                ar = Enumerable.Range(0, DataSize).Select(i => i < 10000 ? (int?)i : null).ToArray();
            } else {
                throw new InvalidOperationException($"don't know about {t}");
            }
            return ar;
        }

        public static DataColumn CreateTestData(DataField f) {
            
            return new DataColumn(f, CreateTestData(f.ClrType));
        }
    }
}

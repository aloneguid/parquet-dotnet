using Parquet.Data;
using Parquet.Schema;

namespace Parquet.PerfRunner.Benchmarks;

public abstract class BenchmarkBase {

    public const int DataSize = 10_000_000;

    private static Random random = new Random();
    public static string RandomString(int length) {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        return new string(Enumerable.Repeat(chars, length)
          .Select(s => s[random.Next(s.Length)]).ToArray());
    }

    public static Array CreateTestData(Type t) {
        Array ar;
        if(t == typeof(int)) {
            ar = Enumerable.Range(0, DataSize).ToArray();
        } else if(t == typeof(int?)) {
            ar = Enumerable.Range(0, DataSize).Select(i => (i % 3 == 0) ? (int?)null : (int?)i).ToArray();
        } else if(t == typeof(double)) {
            ar = Enumerable.Range(0, DataSize).Select(i => (double)i).ToArray();
        } else if(t == typeof(double?)) {
            ar = Enumerable.Range(0, DataSize).Select(i => (i % 3 == 0) ? (double?)null : (double?)i).ToArray();
        } else if(t == typeof(string)) {
            ar = Enumerable.Range(0, DataSize).Select(i => RandomString(30)).ToArray();
        } else {
            throw new InvalidOperationException($"don't know about {t}");
        }
        return ar;
    }

    //public static DataColumn CreateTestData(DataField f) {
        
    //    return new DataColumn(f, CreateTestData(f.ClrType));
    //}
}

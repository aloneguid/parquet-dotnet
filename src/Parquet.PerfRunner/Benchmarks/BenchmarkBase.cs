using System.Buffers;
using CommunityToolkit.HighPerformance.Buffers;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.PerfRunner.Benchmarks;

public abstract class BenchmarkBase {

    public const int DataSize = 10_000_000;

    private static readonly Random random = new Random();
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
        } else if(t == typeof(ReadOnlyMemory<char>)) {
            ar = Enumerable.Range(0, DataSize).Select(i => (ReadOnlyMemory<char>)RandomString(30).AsMemory()).ToArray();
        } else {
            throw new InvalidOperationException($"don't know about {t}");
        }
        return ar;
    }

    public static IMemoryOwner<bool> CreateBools(int count) {
        var owner = MemoryOwner<bool>.Allocate(count);
        Span<bool> span = owner.Memory.Span;
        for(int i = 0; i < count; i++) {
            span[i] = (i % 2 == 0);
        }
        return owner;
    }

    //public static DataColumn CreateTestData(DataField f) {
        
    //    return new DataColumn(f, CreateTestData(f.ClrType));
    //}
}

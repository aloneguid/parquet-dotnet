using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Extensions.Streaming;
using Parquet.Test.Util;
using Xunit;

namespace Parquet.Test;

public class ReadStreamSubStreamTest {
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Async_reads_preserve_bounds_and_position(bool memoryOverload) {
        using var data = new MemoryStream(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 });
        using var source = new AsyncOnlyStream(data, 2);
        using var stream = new ReadStreamSubStream(source, 2, 5);
        using var cancellation = new CancellationTokenSource();
        source.OnReadAsync = token => Assert.Equal(cancellation.Token, token);
        byte[] actual = new byte[8];
        int total = 0;
        int read;
        do {
            read = memoryOverload
                ? await stream.ReadAsync(actual.AsMemory(total), cancellation.Token)
                : await stream.ReadAsync(actual, total, actual.Length - total, cancellation.Token);
            total += read;
            Assert.Equal(total, stream.Position);
        } while(read != 0);

        Assert.Equal(5, total);
        Assert.Equal(new byte[] { 2, 3, 4, 5, 6, 0, 0, 0 }, actual);
        Assert.Equal(7, source.Position);
        stream.Seek(-2, SeekOrigin.End);
        Assert.Equal(2, await stream.ReadAsync(actual.AsMemory(), cancellation.Token));
        Assert.Equal(5, actual[0]);
        Assert.Equal(6, actual[1]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Async_reads_forward_cancellation_without_advancing_position(bool memoryOverload) {
        using var data = new MemoryStream(new byte[] { 0, 1, 2 });
        using var source = new AsyncOnlyStream(data);
        using var stream = new ReadStreamSubStream(source, 1, 2);
        using var cancellation = new CancellationTokenSource();
        source.OnReadAsync = token => {
            Assert.Equal(cancellation.Token, token);
            cancellation.Cancel();
        };
        byte[] buffer = new byte[2];

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => {
            int read = memoryOverload
                ? await stream.ReadAsync(buffer.AsMemory(), cancellation.Token)
                : await stream.ReadAsync(buffer, 0, buffer.Length, cancellation.Token);
            Assert.Equal(buffer.Length, read);
        });
        Assert.Equal(0, stream.Position);
    }
}

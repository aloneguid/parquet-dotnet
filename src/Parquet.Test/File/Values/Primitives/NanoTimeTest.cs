using System;
using System.Collections.Generic;
using Parquet.File.Values.Primitives;
using Xunit;

namespace Parquet.Test.File.Values.Primitives {
    public class NanoTimeTest {
        [Theory]
        [InlineData(637566187511234567)] // 2021-05-14T17:52:31.1234567+00:00 - Works before fix
        [InlineData(637807514459104071)] // 2022-02-18T02:24:05.9104071+00:00 - Does not work before fix
        [MemberData(nameof(GenerateRandomDateTime), 1000)] // Generate a bunch of random date times
        public void ConvertToDateTimeOffset_PreservesTicks(long ticks) {
            DateTimeOffset dto = new DateTime(ticks).ToUniversalTime();
            var nanoTime = new NanoTime(dto);
            var convertedDto = (DateTimeOffset)nanoTime;

            Assert.Equal(dto, convertedDto);
        }

        public static IEnumerable<object[]> GenerateRandomDateTime(int num) {
            var rand = new Random();
            for(int i = 0; i < num; i++) {
                var dateTime = new DateTimeOffset(2022, rand.Next(1, 13), rand.Next(1, 28), rand.Next(0, 24),
                    rand.Next(0, 60), rand.Next(0, 60), rand.Next(0, 1000), TimeSpan.Zero);
                dateTime = dateTime.AddTicks(rand.Next(0, (int)TimeSpan.TicksPerMillisecond));
                yield return new object[] { dateTime.Ticks };
            }
        }
    }
}
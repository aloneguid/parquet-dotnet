using System;
using System.Collections.Generic;
using Parquet.File.Values.Primitives;
using Xunit;

namespace Parquet.Test.File.Values.Primitives {
    public class NanoTimeTest {
        [Theory]
        [InlineData(637566187511234567)] // 2021-05-14T17:52:31.1234567+00:00 - Works before fix
        [InlineData(637807514459104071)] // 2022-02-18T02:24:05.9104071+00:00 - Does not work before fix
        //[MemberData(nameof(GenerateRandomDateTime), 100)] // Generate a bunch of random date times
        public void ConvertToDateTimeOffset_PreservesTicks(long ticks) {
            var dto = new DateTime(ticks, DateTimeKind.Utc);
            var nanoTime = new NanoTime(dto);
            var convertedDto = (DateTime)nanoTime;

            Assert.Equal(dto, convertedDto);
        }
    }
}
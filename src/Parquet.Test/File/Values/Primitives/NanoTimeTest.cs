using System;
using Parquet.File.Values.Primitives;
using Xunit;

namespace Parquet.Test.File.Values.Primitives
{
   public class NanoTimeTest
   {
      [Fact]
      public void ConvertToDateTimeOffset_PreservesTicks()
      {
         var dto = new DateTimeOffset(2021, 05, 14, 17, 52, 31, TimeSpan.Zero);
         dto = dto.Add(TimeSpan.FromTicks(1234567));
         var nanoTime = new NanoTime(dto);
         var convertedDto = (DateTimeOffset) nanoTime;

         Assert.Equal(dto, convertedDto);
      }
   }
}
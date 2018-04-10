using System;
using System.IO;
using SharpArrow.Data;
using Xunit;

namespace SharpArrow.Test
{
   public class DiskReadTest : TestBase
   {
      [Fact]
      public void Smoke_that_file()
      {
         using (Stream s = GetDataFileStream("file.dat"))
         {
            using (var af = new ArrowFile(s))
            {
               Schema schema = af.Schema;
            }
         }
      }
   }
}

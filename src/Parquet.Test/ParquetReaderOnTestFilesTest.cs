using Parquet.Data;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Parquet.Test
{
   /// <summary>
   /// Tests a set of predefined test files that they read back correct
   /// </summary>
   public class ParquetReaderOnTestFilesTest : TestBase
   {
      private byte[] vals = new byte[18]
      {
         0x00,
         0x00,
         0x27,
         0x79,
         0x7f,
         0x26,
         0xd6,
         0x71,
         0xc8,
         0x00,
         0x00,
         0x4e,
         0xf2,
         0xfe,
         0x4d,
         0xac,
         0xe3,
         0x8f
      };

      [Fact]
      public async Task FixedLenByteArray_dictionaryAsync()
      {
         using (Stream s = OpenTestFile("fixedlenbytearray.parquet"))
         {
            await using (var r = new ParquetReader(s))
            {
               DataColumn[] columns = await r.ReadEntireRowGroupAsync().ConfigureAwait(false);
            }
         }
      }

      [Fact]
      public async Task Datetypes_allAsync()
      {
         DateTimeOffset offset, offset2;
         using (Stream s = OpenTestFile("dates.parquet"))
         {
            await using (var r = new ParquetReader(s))
            {
               DataColumn[] columns = await r.ReadEntireRowGroupAsync().ConfigureAwait(false);

               offset = (DateTimeOffset)(columns[1].Data.GetValue(0));
               offset2 = (DateTimeOffset)(columns[1].Data.GetValue(1));
            }
         }
         Assert.Equal(new DateTime(2017, 1, 1), offset.Date);
         Assert.Equal(new DateTime(2017, 2, 1), offset2.Date);
      }

      [Fact]
      public async Task DateTime_FromOtherSystemAsync()
      {
         DateTimeOffset offset;
         using (Stream s = OpenTestFile("datetime_other_system.parquet"))
         {
            await using (var r = new ParquetReader(s))
            {
               DataColumn[] columns = await r.ReadEntireRowGroupAsync().ConfigureAwait(false);

               DataColumn as_at_date_col = columns.FirstOrDefault(x => x.Field.Name == "as_at_date_");
               Assert.NotNull(as_at_date_col);

               offset = (DateTimeOffset)(as_at_date_col.Data.GetValue(0));
               Assert.Equal(new DateTime(2018, 12, 14, 0, 0, 0), offset.Date);
            }
         }
      }
   }
}

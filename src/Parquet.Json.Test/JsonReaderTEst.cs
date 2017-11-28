using Newtonsoft.Json.Linq;
using Parquet.Data;
using Xunit;

namespace Parquet.Json.Test
{
   public class JsonReaderTest : TestBase
   {
      [Fact]
      public void Read_simple_json_with_no_nesting()
      {
         JObject jo = JObject.Parse(ReadJson("001.json"));

         DataSet ds = jo.ToParquetDataSet();

         //validate schema
         Schema s = ds.Schema;
         Assert.Equal(3, s.Length);
         Assert.Equal(new DataField<int?>("id"), s[0]);
         Assert.Equal(new DataField<string>("country"), s[1]);
         Assert.Equal(new DataField<int?>("population"), s[2]);

         //validate data
         Assert.Equal(0, ds.RowCount);
      }

   }
}
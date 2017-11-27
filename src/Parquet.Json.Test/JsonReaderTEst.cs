using System;
using Newtonsoft.Json;
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
      }

   }
}
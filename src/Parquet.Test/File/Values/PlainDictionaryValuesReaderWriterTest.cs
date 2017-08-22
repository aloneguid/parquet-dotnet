using System.Collections.Generic;
using Xunit;
using Parquet.File.Values;
using Parquet.Data;
using System.IO;
using System.Collections;

namespace Parquet.Test.File.Values
{
   public class PlainDictionaryValuesReaderWriterTest
   {
      [Fact]
      public void Dictionary_detects_uniqueue_values()
      {
         var writer = new PlainDictionaryValuesWriter(new RunLengthBitPackingHybridValuesWriter());
         var schema = new SchemaElement<string>("s");

         var ms = new MemoryStream();
         var bw = new BinaryWriter(ms);

         writer.Write(bw, schema,
            new List<string>
            {
               "one",
               "one",
               "one",
               "two",
               "one",
               "one"
            },
            out IList dictionary);

         Assert.Equal(2, dictionary.Count);
      }
   }
}

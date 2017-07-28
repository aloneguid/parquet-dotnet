using System;
using System.Collections.Generic;
using System.Text;
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
      public void Smoke()
      {
         var writer = new PlainDictionaryValuesWriter();
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
      }
   }
}

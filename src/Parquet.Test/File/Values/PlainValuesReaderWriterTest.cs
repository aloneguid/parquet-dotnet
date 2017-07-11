using Parquet.Data;
using Parquet.File.Values;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace Parquet.Test.File.Values
{
   public class PlainValuesReaderWriterTest
   {
      private ParquetOptions _options;
      private IValuesReader _reader;
      private IValuesWriter _writer;
      private BinaryWriter _bw;
      private BinaryReader _br;
      private MemoryStream _ms;

      public PlainValuesReaderWriterTest()
      {
         _options = new ParquetOptions();

         _reader = new PlainValuesReader(_options);
         _writer = new PlainValuesWriter(_options);

         _ms = new MemoryStream();
         _br = new BinaryReader(_ms);
         _bw = new BinaryWriter(_ms);
      }

      [Fact]
      public void Array_of_booleans_writes_and_reads()
      {
         var bools = new List<bool>();
         bools.AddRange(new[] { true, false, true });

         var schema = new SchemaElement<bool>("bool");

         _writer.Write(_bw, schema, bools);

         _ms.Position = 0;

         var boolsRead = new List<bool?>();
         _reader.Read(_br, schema, boolsRead, 3);

         Assert.Equal(bools, boolsRead.Cast<bool>().ToList());
      }

      [Fact]
      public void DateTimeOffset_writes_and_reads()
      {
         var dates = new List<DateTimeOffset> { DateTime.UtcNow.RoundToSecond() };  //this version doesn't store milliseconds
         var schema = new SchemaElement<DateTimeOffset>("dto");

         _writer.Write(_bw, schema, dates);

         _ms.Position = 0;

         var datesRead = new List<DateTimeOffset>();
         _reader.Read(_br, schema, datesRead, 1);

         Assert.True((dates[0] - datesRead[0]).TotalDays <= 1);
      }
   }
}

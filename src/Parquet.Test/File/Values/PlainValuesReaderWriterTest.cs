using Parquet.File.Values;
using Parquet.Thrift;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
      public void Booleans()
      {
         var bools = new List<bool>();
         bools.AddRange(new[] { true, false, true });

         var schema = new SchemaElement();
         schema.Type = Thrift.Type.BOOLEAN;

         _writer.Write(_bw, schema, bools);

         _ms.Position = 0;

         var boolsRead = new List<bool?>();
         _reader.Read(_br, schema, boolsRead, 3);

         Assert.Equal(bools, boolsRead.Cast<bool>().ToList());
      }
   }
}

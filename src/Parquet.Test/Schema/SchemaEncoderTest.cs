using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Encodings;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Schema {
    public class SchemaEncoderTest {
        [Fact]
        public void Encode_decimal_38_flba() {
            var f38 = new DecimalDataField("d", 38, 18);
            Meta.SchemaElement se = SchemaEncoder.Encode(f38);

            int i = 0;
            Field f = SchemaEncoder.Decode(new List<Meta.SchemaElement> { se }, new ParquetOptions(), ref i, out _)!;
            Assert.Equal(typeof(decimal), ((DataField)f).ClrType);
        }

        [Fact]
        public void Encode_uuid() {
            var uuid = new DataField<Guid>("u");
            Meta.SchemaElement se = SchemaEncoder.Encode(uuid);

            int i = 0;
            Field f = SchemaEncoder.Decode(new List<Meta.SchemaElement> { se }, new ParquetOptions(), ref i, out _)!;
            Assert.Equal(typeof(Guid), ((DataField)f).ClrType);
        }
    }
}

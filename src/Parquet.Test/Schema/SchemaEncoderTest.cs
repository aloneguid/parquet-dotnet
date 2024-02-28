using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Encodings;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Schema {
    public class SchemaEncoderTest {
        [Theory]
        [InlineData(38, 18)]
        [InlineData(29, 6)]
        public void Encode_decimal(int precision, int scale) {
            var f38 = new DecimalDataField("d", precision, scale);
            Meta.SchemaElement se = SchemaEncoder.Encode(f38);

            int i = 0;
            Field f = SchemaEncoder.Decode(new List<Meta.SchemaElement> { se }, new ParquetOptions(), ref i, out _)!;
            Assert.Equal(typeof(decimal), ((DataField)f).ClrType);
            Assert.IsType<DecimalDataField>(f);
            Assert.Equal(precision, ((DecimalDataField)f).Precision);
            Assert.Equal(scale, ((DecimalDataField)f).Scale);
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

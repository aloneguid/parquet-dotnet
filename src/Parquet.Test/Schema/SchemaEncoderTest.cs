using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Encodings;
using Parquet.Meta;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Schema {
    public class SchemaEncoderTest {
        [Theory]
        [InlineData(38, 18)]
        [InlineData(29, 6)]
        public void Encode_decimal(int precision, int scale) {
            var f38 = new DecimalDataField("d", precision, scale);
            Meta.SchemaElement se = SchemaEncoder.Encode(f38, new ParquetOptions());

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
            Meta.SchemaElement se = SchemaEncoder.Encode(uuid, new ParquetOptions());

            int i = 0;
            Field f = SchemaEncoder.Decode(new List<Meta.SchemaElement> { se }, new ParquetOptions(), ref i, out _)!;
            Assert.Equal(typeof(Guid), ((DataField)f).ClrType);
        }

        [Fact]
        public void Encode_ListOfOptionalAndRequiredStructs() {
            var schema = new ParquetSchema(
                new DataField<int>("id"),
               new ListField("list1",
                  new StructField("item",
                     new DataField<int>("id"),
                     new DataField<string>("name")
                  )
               ),
               new ListField("list2",
                  new StructField("item",
                     new DataField<int>("id"),
                     new DataField<string>("name")
                  ) { IsNullable = false }
               )
            );

            // check repetition and definition levels on generated struct

            // CLASS
            var root = new SchemaElement { Name = "root" };
            var lst = new List<SchemaElement>();
            SchemaEncoder.Encode(schema[1], root, lst, new ParquetOptions());

            Assert.Equal(FieldRepetitionType.OPTIONAL, lst[0].RepetitionType);  // LIST
            Assert.Equal(FieldRepetitionType.REPEATED, lst[1].RepetitionType);  // thumb
            Assert.Equal(FieldRepetitionType.OPTIONAL, lst[2].RepetitionType);  // item

            // STRUCT
            root = new SchemaElement { Name = "root" };
            lst = new List<SchemaElement>();
            SchemaEncoder.Encode(schema[2], root, lst, new ParquetOptions());

            Assert.Equal(FieldRepetitionType.OPTIONAL, lst[0].RepetitionType);  // LIST
            Assert.Equal(FieldRepetitionType.REPEATED, lst[1].RepetitionType);  // thumb
            Assert.Equal(FieldRepetitionType.REQUIRED, lst[2].RepetitionType);  // item
        }
    }
}

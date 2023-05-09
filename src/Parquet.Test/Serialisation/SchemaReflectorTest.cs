using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using Parquet.Schema;
using Parquet.Serialization;
using Parquet.Serialization.Attributes;
using Xunit;

namespace Parquet.Test.Serialisation {
    public class SchemaReflectorTest : TestBase {


        /// <summary>
        /// Essentially all the test cases are this class' fields
        /// </summary>
        class PocoClass {
            public int Id { get; set; }

            [JsonPropertyName("AltId")] public int AnnotatedId { get; set; }
            public float? NullableFloat { get; set; }

            public int[]? IntArray { get; set; }
        }

        [Fact]
        public void I_can_infer_different_types() {
            ParquetSchema schema = typeof(PocoClass).GetParquetSchema(true);

            Assert.NotNull(schema);
            Assert.Equal(4, schema.Fields.Count);

            // verify

            DataField id = (DataField)schema[0];
            Assert.Equal("Id", id.Name);
            Assert.Equal(typeof(int), id.ClrType);
            Assert.False(id.IsNullable);
            Assert.False(id.IsArray);

            DataField altId = (DataField)schema[1];
            Assert.Equal("AltId", altId.Name);
            Assert.Equal(typeof(int), id.ClrType);
            Assert.False(id.IsNullable);
            Assert.False(id.IsArray);

            DataField nullableFloat = (DataField)schema[2];
            Assert.Equal("NullableFloat", nullableFloat.Name);
            Assert.Equal(typeof(float), nullableFloat.ClrType);
            Assert.True(nullableFloat.IsNullable);
            Assert.False(nullableFloat.IsArray);

            ListField intArray = (ListField)schema[3];
            Assert.Equal("IntArray", intArray.Name);
            Assert.True(intArray.IsNullable);

            DataField intArrayData = (DataField)intArray.Item;
            Assert.Equal(typeof(int), intArrayData.ClrType);
            Assert.False(intArrayData.IsNullable);
            Assert.False(intArrayData.IsArray);
        }


        class PocoSubClass : PocoClass {
            public int ExtraProperty { get; set; }
        }

        [Fact]
        public void I_can_recognize_inherited_properties() {
            ParquetSchema schema = typeof(PocoSubClass).GetParquetSchema(true);
            Assert.Equal(5, schema.Fields.Count);
            DataField extraProperty = (DataField)schema[0];
            Assert.Equal("ExtraProperty", extraProperty.Name);
            Assert.Equal(typeof(int), extraProperty.ClrType);
            Assert.False(extraProperty.IsNullable);
            Assert.False(extraProperty.IsArray);

            DataField id = (DataField)schema[1];
            Assert.Equal("Id", id.Name);
            Assert.Equal(typeof(int), id.ClrType);
            Assert.False(id.IsNullable);
            Assert.False(id.IsArray);

            DataField altId = (DataField)schema[2];
            Assert.Equal("AltId", altId.Name);
            Assert.Equal(typeof(int), id.ClrType);
            Assert.False(id.IsNullable);
            Assert.False(id.IsArray);

            DataField nullableFloat = (DataField)schema[3];
            Assert.Equal("NullableFloat", nullableFloat.Name);
            Assert.Equal(typeof(float), nullableFloat.ClrType);
            Assert.True(nullableFloat.IsNullable);
            Assert.False(nullableFloat.IsArray);

            ListField intArray = (ListField)schema[4];
            Assert.Equal("IntArray", intArray.Name);
            Assert.True(intArray.IsNullable);

            var extraProp = (DataField)schema[0];
            Assert.Equal("ExtraProperty", extraProp.Name);
            Assert.Equal(typeof(int), extraProp.ClrType);
            Assert.False(extraProp.IsNullable);
            Assert.False(extraProp.IsArray);
        }

        class AliasedPoco {

            [JsonPropertyName("ID1")]
            public int _id1 { get; set; }

            [JsonPropertyName("ID2")]
            public int _id2 { get; set; }
        }

        [Fact]
        public void AliasedProperties() {
            ParquetSchema schema = typeof(AliasedPoco).GetParquetSchema(true);

            Assert.Equal(new ParquetSchema(
                new DataField<int>("ID1"),
                new DataField<int>("ID2")
                ), schema);
        }

#pragma warning disable CS0618 // Type or member is obsolete

        class IgnoredPoco {

            public int NotIgnored { get; set; }

            [JsonIgnore]
            public int Ignored1 { get; set; }

            [JsonIgnore]
            public int Ignored2 { get; set; }
        }
#pragma warning restore CS0618 // Type or member is obsolete


        [Fact]
        public void IgnoredProperties() {
            ParquetSchema schema = typeof(IgnoredPoco).GetParquetSchema(true);

            Assert.Equal(new ParquetSchema(
                new DataField<int>("NotIgnored")), schema);
        }

        public class UnorderedProps {
            public int Id { get; set; }

            public string? Name { get; set; }
        }

        [Fact]
        public void Fields_in_class_order() {
            ParquetSchema schema = typeof(UnorderedProps).GetParquetSchema(true);

            Assert.Equal(new ParquetSchema(new DataField<int>("Id"), new DataField<string>("Name")), schema);
        }

#if !NETCOREAPP3_1

        public class OrderedProps {

            [JsonPropertyOrder(1)]
            public int Id { get; set; }

            [JsonPropertyOrder(0)]
            public string? Name { get; set; }
        }

        [Fact]
        public void Fields_in_reorder_order() {
            ParquetSchema schema = typeof(OrderedProps).GetParquetSchema(true);

            Assert.Equal(new ParquetSchema(new DataField<string>("Name"), new DataField<int>("Id")), schema);
        }

#endif

        class SimpleMapPoco {
            public int? Id { get; set; }

            public Dictionary<string, int> Tags { get; set; } = new Dictionary<string, int>();
        }

        [Fact]
        public void SimpleMap() {
            ParquetSchema schema = typeof(SimpleMapPoco).GetParquetSchema(true);

            Assert.Equal(new ParquetSchema(
                new DataField<int?>("Id"),
                new MapField("Tags", 
                    new DataField<string>("Key"),
                    new DataField<int>("Value"))), schema);
        }

        class StructMemberPoco {
            public string? FirstName { get; set; }

            public string? LastName { get; set; }
        }

        class StructMasterPoco {
            public int Id { get; set; }

            public StructMemberPoco? Name { get; set; }
        }

        [Fact]
        public void SimpleStruct() {
            ParquetSchema schema = typeof(StructMasterPoco).GetParquetSchema(true);

            Assert.Equal(new ParquetSchema(
                new DataField<int>("Id"),
                new StructField("Name",
                    new DataField<string>("FirstName"),
                    new DataField<string>("LastName")
                )), schema);
        }

        class ListOfStructsPoco {
            public int Id { get; set; }

            public List<StructMemberPoco>? Members { get; set; }
        }

        [Fact]
        public void ListOfStructs() {
            ParquetSchema actualSchema = typeof(ListOfStructsPoco).GetParquetSchema(true);

            var expectedSchema = new ParquetSchema(
                new DataField<int>("Id"),
                new ListField("Members",
                    new StructField("element",
                        new DataField<string>("FirstName"),
                        new DataField<string>("LastName"))));

            Assert.True(
                expectedSchema.Equals(actualSchema),
                expectedSchema.GetNotEqualsMessage(actualSchema, "expected", "actual"));
        }

        class DatesPoco {

            public DateTime ImpalaDate { get; set; }

            [ParquetTimestamp]
            public DateTime TimestampDate { get; set; }

            public TimeSpan DefaultTime { get; set; }

            [ParquetMicroSecondsTime]
            public TimeSpan MicroTime { get; set; }
        }

        [Fact]
        public void Date_default_impala() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            Assert.True(s.DataFields[0] is DateTimeDataField);
            Assert.Equal(DateTimeFormat.Impala, ((DateTimeDataField)s.DataFields[0]).DateTimeFormat);
        }

        [Fact]
        public void Date_timestamp() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            Assert.True(s.DataFields[1] is DateTimeDataField);
            Assert.Equal(DateTimeFormat.DateAndTime, ((DateTimeDataField)s.DataFields[1]).DateTimeFormat);
        }

        [Fact]
        public void Time_default() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            Assert.True(s.DataFields[2] is TimeSpanDataField);
            Assert.Equal(TimeSpanFormat.MilliSeconds, ((TimeSpanDataField)s.DataFields[2]).TimeSpanFormat);
        }

        [Fact]
        public void Time_micros() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            Assert.True(s.DataFields[3] is TimeSpanDataField);
            Assert.Equal(TimeSpanFormat.MicroSeconds, ((TimeSpanDataField)s.DataFields[3]).TimeSpanFormat);
        }



        class DecimalPoco {
            public decimal Default { get; set; }

            [ParquetDecimal(40, 20)]
            public decimal With_40_20 { get; set; }
        }

        [Fact]
        public void Decimal_default() {
            ParquetSchema s = typeof(DecimalPoco).GetParquetSchema(true);

            Assert.True(s.DataFields[0] is DecimalDataField);
            Assert.Equal(38, ((DecimalDataField)s.DataFields[0]).Precision);
            Assert.Equal(18, ((DecimalDataField)s.DataFields[0]).Scale);
        }

        [Fact]
        public void Decimal_override() {
            ParquetSchema s = typeof(DecimalPoco).GetParquetSchema(true);

            Assert.True(s.DataFields[1] is DecimalDataField);
            Assert.Equal(40, ((DecimalDataField)s.DataFields[1]).Precision);
            Assert.Equal(20, ((DecimalDataField)s.DataFields[1]).Scale);
        }

    }
}
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
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

            public bool MarkerField;
        }

        [Fact]
        public void I_can_infer_different_types() {
            ParquetSchema schema = typeof(PocoClass).GetParquetSchema(true);

            Assert.NotNull(schema);
            Assert.Equal(5, schema.Fields.Count);

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

            DataField markerField = (DataField)schema[4];
            Assert.Equal("MarkerField", markerField.Name);
            Assert.Equal(typeof(bool), markerField.ClrType);
            Assert.False(markerField.IsNullable);
            Assert.False(markerField.IsArray);
        }


        class DoubleOnlyPoco : PocoClass {
            public double Double { get; set; }

            public double? NullableDouble { get; set; }
        }

        [Fact]
        public void Type_Double() {
            ParquetSchema schema = typeof(DoubleOnlyPoco).GetParquetSchema(true);
            DataField df = schema.FindDataField("Double");
            Assert.False(df.IsNullable);
        }

        [Fact]
        public void Type_Double_Nullable() {
            ParquetSchema schema = typeof(DoubleOnlyPoco).GetParquetSchema(true);
            DataField df = schema.FindDataField("NullableDouble");
            Assert.True(df.IsNullable);
        }

        class PocoSubClass : PocoClass {
            public int ExtraProperty { get; set; }
        }

        [Fact]
        public void I_can_recognize_inherited_properties() {
            ParquetSchema schema = typeof(PocoSubClass).GetParquetSchema(true);
            Assert.Equal(6, schema.Fields.Count);
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

        class AliasedPocoChild
        {
            [JsonPropertyName("ChildID")]
            public int _id { get; set; }
        }

        class AliasedPoco {
            [JsonPropertyName("ID1")]
            public int _id1 { get; set; }

            [JsonPropertyName("ID2")]
            public int _id2 { get; set; }

            [JsonPropertyName("Child")]
            public AliasedPocoChild? _child { get; set; }

            [JsonPropertyName("Numbers")]
            public List<int>? _numberList { get; set; }
        }

        [Fact]
        public void AliasedProperties() {
            ParquetSchema schema = typeof(AliasedPoco).GetParquetSchema(true);

            Assert.Equal(new ParquetSchema(
                new DataField<int>("ID1"),
                new DataField<int>("ID2"),
                new StructField("Child", new DataField<int>("ChildID")),
                new ListField("Numbers", new DataField<int>("element"))
                ), schema);
        }

        class PrimitivesListPoco {
            public List<int>? IntList { get; set; }

            [ParquetSimpleRepeatable]
            public List<int>? LegacyIntList { get; set; }
        }


        [Fact]
        public void ListsOfPrimitives() {
            ParquetSchema schema = typeof(PrimitivesListPoco).GetParquetSchema(true);

            Assert.Equal(new ParquetSchema(
                new ListField("IntList", new DataField<int>("element")),
                new DataField<int[]>("LegacyIntList")),
                schema);
        }

#pragma warning disable CS0618 // Type or member is obsolete

        class IgnoredPoco {

            public int NotIgnored { get; set; }

            [JsonIgnore]
            public int Ignored1 { get; set; }

            [JsonIgnore]
            public int Ignored2 { get; set; }

            [ParquetIgnore]
            public int Ignored3 { get; set; }
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
                    new DataField<string>("key", false),
                    new DataField<int>("value"))), schema);
        }

        [Fact]
        public void MapKeyMetadataIsSetToRequired() {
            ParquetSchema schema = typeof(SimpleMapPoco).GetParquetSchema(true);

            Assert.Equal("key", schema.DataFields[1].Name);
            Assert.False(schema.DataFields[1].IsNullable);
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

            ListField lf = (ListField)actualSchema[1];
            Assert.Equal(1, lf.MaxRepetitionLevel);
            Assert.Equal(2, lf.MaxDefinitionLevel);

            DataField df = (DataField)lf.Item.Children[0];
            Assert.Equal(1, df.MaxRepetitionLevel);
            Assert.Equal(4, df.MaxDefinitionLevel);

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

        class RequiredListOfStructsPoco {
            public int Id { get; set; }

            [ParquetRequired, ParquetListElementRequired]
            public List<StructMemberPoco>? Members { get; set; }
        }

        [Fact]
        public void ListOfStructsRequired() {
            ParquetSchema actualSchema = typeof(RequiredListOfStructsPoco).GetParquetSchema(true);

            ListField lf = (ListField)actualSchema[1];

            DataField df = (DataField)lf.Item.Children[0];
            Assert.Equal(1, df.MaxRepetitionLevel);
            Assert.Equal(2, df.MaxDefinitionLevel);
        }


        class DatesPoco {

            public DateTime ImpalaDate { get; set; }

            public DateTime? NullableImpalaDate { get; set; }

            [ParquetTimestamp]
            public DateTime TimestampDate { get; set; }

            [ParquetTimestamp(useLogicalTimestamp: true, isAdjustedToUTC: false)]
            public DateTime LogicalLocalTimestampDate { get; set; }
            
            [ParquetTimestamp(useLogicalTimestamp: true)]
            public DateTime LogicalUtcTimestampDate { get; set; }

            [ParquetTimestamp]
            public DateTime? NullableTimestampDate { get; set; }

            public TimeSpan DefaultTime { get; set; }
            
            public TimeSpan? NullableTimeSpan { get; set; }

            [ParquetMicroSecondsTime]
            public TimeSpan MicroTime { get; set; }
            
#if NET6_0_OR_GREATER
            public DateOnly ImpalaDateOnly { get; set; }

            public TimeOnly DefaultTimeOnly { get; set; }

            public TimeOnly? DefaultNullableTimeOnly { get; set; }

            [ParquetMicroSecondsTime]
            public TimeOnly MicroTimeOnly { get; set; }

            public DateOnly? NullableDateOnly { get; set; }

            public TimeOnly? NullableTimeOnly { get; set; }
#endif

#if NET7_0_OR_GREATER
            [ParquetTimestamp(ParquetTimestampResolution.Microseconds)]
            public DateTime TimestampMicrosDate { get; set; }
#endif
        }

        [Fact]
        public void Type_DateTime_DefaultImpala() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);
            DataField df = s.FindDataField(nameof(DatesPoco.ImpalaDate));

            Assert.True(df is DateTimeDataField);
            Assert.Equal(DateTimeFormat.Impala, ((DateTimeDataField)df).DateTimeFormat);
            Assert.False(df.IsNullable);
        }

        [Fact]
        public void Type_DateTime_NullableImpala() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);
            DataField df = s.FindDataField(nameof(DatesPoco.NullableImpalaDate));

            Assert.True(df is DateTimeDataField);
            Assert.Equal(DateTimeFormat.Impala, ((DateTimeDataField)df).DateTimeFormat);
            Assert.True(df.IsNullable);
        }

        [Fact]
        public void Type_DateTime_Timestamp() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            DataField df = s.FindDataField(nameof(DatesPoco.TimestampDate));
            Assert.True(df is DateTimeDataField);
            Assert.Equal(DateTimeFormat.DateAndTime, ((DateTimeDataField)df).DateTimeFormat);
        }

        [Fact]
        public void Type_DateTime_LogicalLocalTimestamp() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            DataField df = s.FindDataField(nameof(DatesPoco.LogicalLocalTimestampDate));
            Assert.True(df is DateTimeDataField);
            Assert.False(((DateTimeDataField)df).IsAdjustedToUTC);
            Assert.Equal(DateTimeFormat.Timestamp, ((DateTimeDataField)df).DateTimeFormat);
        }
        
        [Fact]
        public void Type_DateTime_LogicalUtcTimestamp() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            DataField df = s.FindDataField(nameof(DatesPoco.LogicalUtcTimestampDate));
            Assert.True(df is DateTimeDataField);
            Assert.True(((DateTimeDataField)df).IsAdjustedToUTC);
            Assert.Equal(DateTimeFormat.Timestamp, ((DateTimeDataField)df).DateTimeFormat);
        }

        [Fact]
        public void Type_DateTime_TimestampNullable() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            DataField df = s.FindDataField(nameof(DatesPoco.NullableTimestampDate));
            Assert.True(df is DateTimeDataField);
            Assert.Equal(DateTimeFormat.DateAndTime, ((DateTimeDataField)df).DateTimeFormat);
        }

        [Fact]
        public void Type_TimeSpan_Default() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            DataField df = s.FindDataField(nameof(DatesPoco.DefaultTime));
            Assert.True(df is TimeSpanDataField);
            Assert.Equal(TimeSpanFormat.MilliSeconds, ((TimeSpanDataField)df).TimeSpanFormat);
        }
        
        [Fact]
        public void Type_TimeSpan_Nullable() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            DataField df = s.FindDataField(nameof(DatesPoco.NullableTimeSpan));
            Assert.True(df is TimeSpanDataField);
            Assert.True(df.IsNullable);
            Assert.Equal(TimeSpanFormat.MilliSeconds, ((TimeSpanDataField)df).TimeSpanFormat);
        }

        [Fact]
        public void Type_TimeSpan_Micros() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            DataField df = s.FindDataField(nameof(DatesPoco.MicroTime));
            Assert.True(df is TimeSpanDataField);
            Assert.Equal(TimeSpanFormat.MicroSeconds, ((TimeSpanDataField)df).TimeSpanFormat);
        }

#if NET6_0_OR_GREATER
        [Fact]
        public void Type_DateOnly_Timestamp() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            DataField df = s.FindDataField(nameof(DatesPoco.ImpalaDateOnly));
            Assert.True(df.GetType() == typeof(DataField));
            Assert.Equal(SchemaType.Data, df.SchemaType);
            Assert.Equal(typeof(DateOnly), df.ClrType);
            Assert.False(df.IsNullable);
        }
        
        [Fact]
        public void Type_TimeOnly_Default() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            DataField df = s.FindDataField(nameof(DatesPoco.DefaultTimeOnly));
            Assert.True(df is TimeOnlyDataField);
            Assert.Equal(TimeSpanFormat.MilliSeconds, ((TimeOnlyDataField)df).TimeSpanFormat);
            Assert.False(df.IsNullable);
        }

        [Fact]
        public void Type_TimeOnly_Nullable() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            DataField df = s.FindDataField(nameof(DatesPoco.DefaultNullableTimeOnly));
            Assert.True(df is TimeOnlyDataField);
            Assert.Equal(TimeSpanFormat.MilliSeconds, ((TimeOnlyDataField)df).TimeSpanFormat);
            Assert.True(df.IsNullable);
        }

        [Fact]
        public void Type_TimeOnly_Micros() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);

            DataField df = s.FindDataField(nameof(DatesPoco.MicroTimeOnly));
            Assert.True(df is TimeOnlyDataField);
            Assert.Equal(TimeSpanFormat.MicroSeconds, ((TimeOnlyDataField)df).TimeSpanFormat);
        }

        [Fact]
        public void Type_DateOnly_NullableTimestamp() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);
            DataField df = s.FindDataField(nameof(DatesPoco.NullableDateOnly));
            Assert.True(df.GetType() == typeof(DataField));
            Assert.Equal(SchemaType.Data, df.SchemaType);
            Assert.Equal(typeof(DateOnly), df.ClrType);
            Assert.True(df.IsNullable);
        }

        [Fact]
        public void Type_TimeOnly_NullableTimestamp() {
            ParquetSchema s = typeof(DatesPoco).GetParquetSchema(true);
            DataField df = s.FindDataField(nameof(DatesPoco.NullableTimeOnly));

            Assert.NotNull(df);
            Assert.Equal(nameof(DatesPoco.NullableTimeOnly), df.Name);
            Assert.Equal(typeof(TimeOnly?), df.ClrNullableIfHasNullsType);
            Assert.True(df.IsNullable);
        }


#endif

        class DecimalPoco {
            public decimal Default { get; set; }

            [ParquetDecimal(40, 20)]
            public decimal With_40_20 { get; set; }

            public decimal? NullableDecimal { get; set; }
        }

        [Fact]
        public void Type_Decimal() {
            ParquetSchema s = typeof(DecimalPoco).GetParquetSchema(true);

            Assert.True(s.DataFields[0] is DecimalDataField);
            Assert.Equal(38, ((DecimalDataField)s.DataFields[0]).Precision);
            Assert.Equal(18, ((DecimalDataField)s.DataFields[0]).Scale);
            Assert.False(s.DataFields[0].IsNullable);
        }

        [Fact]
        public void Type_Decimal_override() {
            ParquetSchema s = typeof(DecimalPoco).GetParquetSchema(true);

            Assert.True(s.DataFields[1] is DecimalDataField);
            Assert.Equal(40, ((DecimalDataField)s.DataFields[1]).Precision);
            Assert.Equal(20, ((DecimalDataField)s.DataFields[1]).Scale);
        }


        [Fact]
        public void Type_Decimal_Nullable() {
            ParquetSchema schema = typeof(DecimalPoco).GetParquetSchema(true);
            DataField df = schema.FindDataField("NullableDecimal");
            Assert.True(df.IsNullable);
        }


        public class StringPoco {
            [ParquetRequired]
            public string Id { get; set; } = "";
            public string? Name { get; set; }
            public string? Location { get; set; }
        }

        [Fact]
        public void Strings_OptionalAndRequired() {
            ParquetSchema s = typeof(StringPoco).GetParquetSchema(true);

            Assert.False(s.DataFields[0].IsNullable);
            Assert.True(s.DataFields[1].IsNullable);
            Assert.True(s.DataFields[2].IsNullable);
        }

        public interface IInterface {
            int Id { get; set; }
        }

        [Fact]
        public void InterfaceIsSupported() {
            ParquetSchema s = typeof(IInterface).GetParquetSchema(true);

            Assert.NotNull(s);
            DataField df = s.FindDataField(nameof(IInterface.Id));
            Assert.True(df.GetType() == typeof(DataField));
            Assert.Equal(SchemaType.Data, df.SchemaType);
            Assert.Equal(typeof(int), df.ClrType);
            Assert.False(df.IsNullable);
        }

        public interface IRootInterface {
            IInterface Child { get; set; }
        }

        [Fact]
        public void InterfacePropertyOnInterfaceIsSupported() {
            ParquetSchema s = typeof(IRootInterface).GetParquetSchema(true);

            Assert.NotNull(s);
            Field df = s.Fields[0];
            Assert.True(df.GetType() == typeof(StructField));
            Assert.Equal(SchemaType.Struct, df.SchemaType);
            Assert.True(df.IsNullable);
        }

        public class ReadOnlyProperty {
            public int Id { get; set; }

            public int ReadOnly => 42;
        }

        [Fact]
        public void ReadOnlyPropertyNotWriteable() {
            ParquetSchema sr = typeof(ReadOnlyProperty).GetParquetSchema(false);
            ParquetSchema sw = typeof(ReadOnlyProperty).GetParquetSchema(true);

            Assert.True(sr.Fields.Count == 2);
            Assert.True(sw.Fields.Count == 1);
        }

        public enum DefaultEnum {
            One,
            Two,
            Three
        }

        public enum ShortEnum : short {
            One,
            Two,
            Three
        }

        public class EnumsInClasses {
            public int Id { get; set; }

            public DefaultEnum DE { get; set; }
 
            // Nullable Enum
            public DefaultEnum? NE { get; set; }

            public ShortEnum SE { get; set; }
        }

        [Fact]
        public void Enums() {
            ParquetSchema schema = typeof(EnumsInClasses).GetParquetSchema(true);
            Assert.Equal(4, schema.Fields.Count);

            DataField dedf = schema.FindDataField("DE");
            DataField nedf = schema.FindDataField("NE");
            DataField sedf = schema.FindDataField("SE");

            Assert.Equal(typeof(int), dedf.ClrType);
            Assert.False(dedf.IsNullable);
            Assert.Equal(typeof(int), nedf.ClrType);
            Assert.True(nedf.IsNullable);
            Assert.Equal(typeof(short), sedf.ClrType);
            Assert.False(dedf.IsNullable);
        }

        struct SimpleClrStruct {
            public int Id { get; set; }
        }

        [Fact]
        public void ClrStruct_IsSupported() {
            ParquetSchema schema = typeof(SimpleClrStruct).GetParquetSchema(true);
            Assert.Single(schema.Fields);
            Assert.Equal(typeof(int), schema.DataFields[0].ClrType);
        }

        class StructWithClrStruct {
            public SimpleClrStruct S { get; set; }
        }

        [Fact]
        public void ClrStruct_AsMember_IsSupported() {
            ParquetSchema schema = typeof(StructWithClrStruct).GetParquetSchema(false);
            Assert.Single(schema.Fields);

            // check it's a required struct
            StructField sf = (StructField)schema[0];
            Assert.False(sf.IsNullable, "struct cannot be optional");

            // check the struct field
            Assert.Single(sf.Children);
            var idField = (DataField)sf.Children[0];
            Assert.Equal(typeof(int), idField.ClrType);
        }

        class StructWithNullableClrStruct {
            // as CLR struct is ValueType, this resolves to System.Nullable<SimpleClrStruct>
            public SimpleClrStruct? N { get; set; }
        }

        [Fact]
        public void ClrStruct_AsNullableMember_IsSupported() {
            ParquetSchema schema = typeof(StructWithNullableClrStruct).GetParquetSchema(false);
            Assert.Single(schema.Fields);

            // check it's a required struct
            StructField sf = (StructField)schema[0];
            Assert.True(sf.IsNullable, "struct must be nullable");

            // check the struct field
            Assert.Single(sf.Children);
            var idField = (DataField)sf.Children[0];
            Assert.Equal(typeof(int), idField.ClrType);
        }
    }
}
using System;
using System.Reflection;
using Parquet.Schema;
using Parquet.XTract;
using Xunit;

namespace Parquet.Test.Schema;

public class MssqlLoaderSchemaMappingTest {

    private static readonly Type LoaderType = typeof(SourceTable).Assembly.GetType("Parquet.XTract.MssqlLoader", throwOnError: true)!;

    private static readonly MethodInfo MapMethod = LoaderType.GetMethod("MapMssqlType",
        BindingFlags.NonPublic | BindingFlags.Static) ?? throw new InvalidOperationException("MapMssqlType method was not found.");

    [Fact]
    public void Decimal_column_maps_to_decimal_data_field_with_precision_and_scale() {
        var field = (DecimalDataField)InvokeMap("amount", "decimal", true, 18, 2);

        Assert.Equal("amount", field.Name);
        Assert.Equal(18, field.Precision);
        Assert.Equal(2, field.Scale);
        Assert.True(field.IsNullable);
    }

    [Fact]
    public void DateTime2_column_maps_to_datetime_data_field() {
        var field = (DateTimeDataField)InvokeMap("created_at", "datetime2", false, null, null);

        Assert.Equal("created_at", field.Name);
        Assert.Equal(DateTimeFormat.DateAndTime, field.DateTimeFormat);
        Assert.False(field.IsAdjustedToUTC);
        Assert.False(field.IsNullable);
    }

    [Fact]
    public void Binary_column_maps_to_byte_array_data_field() {
        var field = (DataField)InvokeMap("payload", "varbinary", true, null, null);

        Assert.Equal(typeof(byte[]), field.ClrType);
        Assert.True(field.IsNullable);
    }

    [Fact]
    public void Sql_variant_column_maps_to_string_data_field() {
        var field = (DataField)InvokeMap("payload", "sql_variant", true, null, null);

        Assert.Equal(typeof(string), field.ClrType);
        Assert.True(field.IsNullable);
    }

    [Fact]
    public void All_types_from_reference_table_definition_are_supported() {
        (string SqlType, Type ClrType)[] mappings =
        [
            ("bit", typeof(bool)),
            ("tinyint", typeof(byte)),
            ("smallint", typeof(short)),
            ("int", typeof(int)),
            ("bigint", typeof(long)),
            ("decimal", typeof(decimal)),
            ("numeric", typeof(decimal)),
            ("smallmoney", typeof(decimal)),
            ("money", typeof(decimal)),
            ("float", typeof(double)),
            ("real", typeof(float)),
            ("date", typeof(DateTime)),
            ("time", typeof(TimeOnly)),
            ("datetime", typeof(DateTime)),
            ("datetime2", typeof(DateTime)),
            ("smalldatetime", typeof(DateTime)),
            ("datetimeoffset", typeof(string)),
            ("char", typeof(string)),
            ("varchar", typeof(string)),
            ("text", typeof(string)),
            ("nchar", typeof(string)),
            ("nvarchar", typeof(string)),
            ("ntext", typeof(string)),
            ("binary", typeof(byte[])),
            ("varbinary", typeof(byte[])),
            ("image", typeof(byte[])),
            ("uniqueidentifier", typeof(Guid)),
            ("xml", typeof(string)),
            ("sql_variant", typeof(string)),
            ("geography", typeof(string)),
            ("geometry", typeof(string)),
            ("hierarchyid", typeof(string)),
            ("rowversion", typeof(byte[])),
        ];

        foreach((string sqlType, Type clrType) in mappings) {
            var field = (DataField)InvokeMap("col", sqlType, true, 18, 4);

            Assert.Equal(clrType, field.ClrType);
            Assert.True(field.IsNullable);
        }
    }

    [Fact]
    public void Unsupported_sql_type_throws_not_supported_exception() {
        var exception = Assert.Throws<TargetInvocationException>(() =>
            MapMethod.Invoke(null, ["node", "unknown_mssql_type", false, null, null]));

        Assert.IsType<NotSupportedException>(exception.InnerException);
    }

    private static Field InvokeMap(string columnName, string sqlType, bool isNullable, int? precision, int? scale) {
        object? field = MapMethod.Invoke(null, [columnName, sqlType, isNullable, precision, scale]);
        return Assert.IsAssignableFrom<Field>(field);
    }
}

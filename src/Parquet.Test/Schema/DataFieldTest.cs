using System;
using System.Collections.Generic;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Schema;

public class DataFieldTest {

    [Fact]
    public void SimpleDataField() {
        var f = new DataField("id", typeof(int));
        Assert.Equal("id", f.Name);
        Assert.Equal(typeof(int), f.ClrType);
        Assert.False(f.IsNullable);
        Assert.False(f.IsArray);
    }

    [Fact]
    public void NullableDataField() {
        var f = new DataField("id", typeof(int?));
        Assert.Equal("id", f.Name);
        Assert.Equal(typeof(int), f.ClrType);
        Assert.True(f.IsNullable);
        Assert.False(f.IsArray);
    }

    [Fact]
    public void SimpleNonNullableArrayFromIEnumerableDataField() {
        var f = new DataField("id", typeof(IEnumerable<int>));
        Assert.Equal("id", f.Name);
        Assert.Equal(typeof(int), f.ClrType);
        Assert.False(f.IsNullable);
        Assert.True(f.IsArray);
    }

    [Fact]
    public void SimpleNullableArrayFromIEnumerableDataField() {
        var f = new DataField("id", typeof(IEnumerable<int?>));
        Assert.Equal("id", f.Name);
        Assert.Equal(typeof(int), f.ClrType);
        Assert.True(f.IsNullable);
        Assert.True(f.IsArray);
    }

    [Fact]
    public void NonNullableArrayFromBoolFlagDataField() {
        var f = new DataField("id", typeof(int), null, true);
        Assert.Equal("id", f.Name);
        Assert.Equal(typeof(int), f.ClrType);
        Assert.False(f.IsNullable);
        Assert.True(f.IsArray);
    }

    [Fact]
    public void NullableArrayFromBoolFlagDataField() {
        var f = new DataField("id", typeof(int), true, true);
        Assert.Equal("id", f.Name);
        Assert.Equal(typeof(int), f.ClrType);
        Assert.True(f.IsNullable);
        Assert.True(f.IsArray);
    }

    [Fact]
    public void NullableArrayOverrideToNonNullable() {
        var f = new DataField("id", typeof(IEnumerable<int?>), false);
        Assert.Equal("id", f.Name);
        Assert.Equal(typeof(int), f.ClrType);
        Assert.False(f.IsNullable);
        Assert.True(f.IsArray);
    }

    [Fact]
    public void StringField() {
        var f = new DataField("name", typeof(string));
        Assert.Equal("name", f.Name);
        Assert.Equal(typeof(string), f.ClrType);
        Assert.True(f.IsNullable);
        Assert.False(f.IsArray);
    }

    [Fact]
    public void StringAsRomCharField() {
        var f = new DataField("name", typeof(ReadOnlyMemory<char>));
        Assert.Equal("name", f.Name);
        Assert.Equal(typeof(ReadOnlyMemory<char>), f.ClrType);
        Assert.False(f.IsNullable);
        Assert.False(f.IsArray);
    }
}

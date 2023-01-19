using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Schema {
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
    }
}

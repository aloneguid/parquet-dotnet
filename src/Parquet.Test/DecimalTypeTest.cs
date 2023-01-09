﻿using System;
using Parquet.Data;
using Xunit;
using System.Threading.Tasks;
using Parquet.Schema;
using Parquet.Rows;

namespace Parquet.Test {
    public class DecimalTypeTest : TestBase {
        [Fact]
        public async Task Read_File_As_Table_With_Decimal_Column_Should_Read_File() {
            const int decimalColumnIndex = 4;
            Table table = await ReadTestFileAsTableAsync("test-types-with-decimal.parquet");

            Assert.Equal(1234.56m, table[0].Get<decimal>(decimalColumnIndex));
        }

        [Fact]
        public void Validate_Scale_Zero_Should_Be_Allowed() {
            const int precision = 1;
            const int scale = 0;
            var field = new DecimalDataField("field-name", precision, scale);
            Assert.Equal(scale, field.Scale);
        }

        [Fact]
        public void Validate_Negative_Scale_Should_Throws_Exception() {
            const int precision = 1;
            const int scale = -1;
            ArgumentException ex = Assert.Throws<ArgumentException>(() => new DecimalDataField("field-name", precision, scale));
            Assert.Equal("scale must be zero or a positive integer (Parameter 'scale')", ex.Message);
        }

        [Fact]
        public void Validate_Precision_Zero_Should_Throws_Exception() {
            const int precision = 0;
            const int scale = 1;
            ArgumentException ex = Assert.Throws<ArgumentException>(() => new DecimalDataField("field-name", precision, scale));
            Assert.Equal("precision is required and must be a non-zero positive integer (Parameter 'precision')", ex.Message);
        }

        [Fact]
        public void Validate_Scale_Bigger_Then_Precision_Throws_Exception() {
            const int precision = 3;
            const int scale = 4;
            ArgumentException ex = Assert.Throws<ArgumentException>(() => new DecimalDataField("field-name", precision, scale));
            Assert.Equal("scale must be less than the precision (Parameter 'scale')", ex.Message);
        }
    }
}
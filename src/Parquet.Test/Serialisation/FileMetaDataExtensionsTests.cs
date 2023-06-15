using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.Meta;
using Parquet.Serialization;
using Xunit;
using Type = Parquet.Meta.Type;

namespace Parquet.Test.Serialisation {
    public class FileMetaDataExtensionsTests {
        private static readonly IEnumerable<Type> _primitiveTypes = new[] {
            Type.BOOLEAN,
            Type.INT32,
            Type.INT32,
            Type.INT64,
            Type.INT96,
            Type.FLOAT,
            Type.DOUBLE
        };

        private static readonly IEnumerable<ConvertedType> _byteArrayPrimitiveConvertedTypes = new[] {
            ConvertedType.UTF8,
            ConvertedType.DECIMAL
        };
        
        private static readonly IEnumerable<ConvertedType> _fixLenByteArrayPrimitiveConvertedTypes = new[] {
            ConvertedType.DECIMAL,
            ConvertedType.INTERVAL
        };

        public static IEnumerable<object[]> PrimitiveTypesTestSet = _primitiveTypes
            .Select(type => new object[] { type });

        public static readonly IEnumerable<object[]> ComplexTypesTestSet = Enum.GetValues(typeof(Type))
            .Cast<Type>()
            .Except(_primitiveTypes)
            .Select(type => new object[] { type });

        [Theory]
        [MemberData(nameof(PrimitiveTypesTestSet))]
        public void GetCompabilityOptions_MakeArrays_ForRepeatedPrimitives(Type primitiveType) {
            FileMetaData metadata = CreateMetadata(new() {
                RepetitionType = FieldRepetitionType.REPEATED,
                Type = primitiveType
            });

            ParquetCompabilityOptions result = metadata.GetCompabilityOptions();
            Assert.True(result.HasFlag(ParquetCompabilityOptions.MakeArrays));
        }
        
        [Theory]
        [MemberData(nameof(ComplexTypesTestSet))]
        public void GetCompabilityOptions_MakeArrays_Latest_ForRepeatedComplexTypes(Type complexType) {
            FileMetaData metadata = CreateMetadata(new() {
                RepetitionType = FieldRepetitionType.REPEATED,
                Type = complexType
            });

            ParquetCompabilityOptions result = metadata.GetCompabilityOptions();
            Assert.False(result.HasFlag(ParquetCompabilityOptions.MakeArrays));           
        }
        
        public static IEnumerable<object[]> ByteArrayPrimitivesTestSet = _byteArrayPrimitiveConvertedTypes
            .Select(type => new object[] { type });
        
        public static IEnumerable<object[]> ByteArrayComplexTypesTestSet = Enum.GetValues(typeof(ConvertedType))
            .Cast<ConvertedType>()
            .Except(_byteArrayPrimitiveConvertedTypes)
            .Select(type => new object[] { type });

        [Theory]
        [InlineData(FieldRepetitionType.OPTIONAL)]
        [InlineData(FieldRepetitionType.REQUIRED)]
        public void GetCompabilityOptions_MakeArray_Latest_WhenNotRepeatedField(FieldRepetitionType repetitionType) {
            FileMetaData metadata = CreateMetadata(new() {
                RepetitionType = repetitionType,
                Type = _primitiveTypes.First(),
            });

            ParquetCompabilityOptions result = metadata.GetCompabilityOptions();
            Assert.False(result.HasFlag(ParquetCompabilityOptions.MakeArrays));
        }

        [Theory]
        [MemberData(nameof(ByteArrayPrimitivesTestSet))]
        public void GetCompabilityOptions_MakeArrays_ByteArrayWithPrimitives(ConvertedType convertedType) {
            FileMetaData metadata = CreateMetadata(new() {
                RepetitionType = FieldRepetitionType.REPEATED,
                Type = Type.BYTE_ARRAY,
                ConvertedType = convertedType
            });

            ParquetCompabilityOptions result = metadata.GetCompabilityOptions();
            Assert.True(result.HasFlag(ParquetCompabilityOptions.MakeArrays));
        }
        [Theory]
        [MemberData(nameof(ByteArrayComplexTypesTestSet))]
        public void GetCompabilityOptions_MakeArrays_Latest_ByteArrayWithComplexTypes(ConvertedType convertedType) {
            FileMetaData metadata = CreateMetadata(new() {
                RepetitionType = FieldRepetitionType.REPEATED,
                Type = Type.BYTE_ARRAY,
                ConvertedType = convertedType
            });

            ParquetCompabilityOptions result = metadata.GetCompabilityOptions();
            Assert.False(result.HasFlag(ParquetCompabilityOptions.MakeArrays));
        }
        
        public static IEnumerable<object[]> FixLenByteArrayPrimitivesTestSet = _fixLenByteArrayPrimitiveConvertedTypes
            .Select(type => new object[] { type });
        
        public static IEnumerable<object[]> FixLenByteArrayComplexTypesTestSet = Enum.GetValues(typeof(ConvertedType))
            .Cast<ConvertedType>()
            .Except(_fixLenByteArrayPrimitiveConvertedTypes)
            .Select(type => new object[] { type });
        
        [Theory]
        [MemberData(nameof(FixLenByteArrayPrimitivesTestSet))]
        public void GetCompabilityOptions_MakeArrays_FixLenByteArrayWithPrimitives(ConvertedType convertedType) {
            FileMetaData metadata = CreateMetadata(new() {
                RepetitionType = FieldRepetitionType.REPEATED,
                Type = Type.FIXED_LEN_BYTE_ARRAY,
                ConvertedType = convertedType
            });

            ParquetCompabilityOptions result = metadata.GetCompabilityOptions();
            Assert.True(result.HasFlag(ParquetCompabilityOptions.MakeArrays));
        }
        [Theory]
        [MemberData(nameof(FixLenByteArrayComplexTypesTestSet))]
        public void GetCompabilityOptions_MakeArrays_Latest_FixLenByteArrayWithComplexTypes(ConvertedType convertedType) {
            FileMetaData metadata = CreateMetadata(new() {
                RepetitionType = FieldRepetitionType.REPEATED,
                Type = Type.FIXED_LEN_BYTE_ARRAY,
                ConvertedType = convertedType
            });

            ParquetCompabilityOptions result = metadata.GetCompabilityOptions();
            Assert.False(result.HasFlag(ParquetCompabilityOptions.MakeArrays));
        }

        private static FileMetaData CreateMetadata(SchemaElement element)
            => new() { Schema = new List<SchemaElement> { element } };
    }
}
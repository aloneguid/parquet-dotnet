using System.Collections.Generic;
using Parquet.Schema;
using Parquet.Serialization;
using Parquet.Serialization.Dremel;
using Parquet.Test.Serialisation.Paper;
using Xunit;

namespace Parquet.Test.Serialisation {

    /// <summary>
    /// These tests validate repetition and definition levels are generated correctly according to the main
    /// Dremel Paper written by google. 
    /// Link to original paper: https://research.google/pubs/pub36632/
    /// </summary>
    public class StriperTest {

        private readonly Document _r1 = new() {
            DocId = 10,
            Links = new Links {
                Forward = new List<long> { 20, 40, 60 }
            },
            Name = new List<Name> {
                    new Name {
                        Language = new List<Language> {
                            new Language {
                                Code = "en-us",
                                Country = "us"
                            },
                            new Language {
                                Code = "en"
                            }
                        },
                        Url = "http://A"
                    },
                    new Name {
                        Url = "http://B"
                    },
                    new Name {
                        Language = new List<Language> {
                            new Language {
                                Code = "en-gb",
                                Country = "gb"
                            }
                        }
                    }
                }
        };

        private readonly Document _r2 = new() {
            DocId = 20,
            Links = new Links {
                Backward = new List<long> { 10, 30 },
                Forward = new List<long> { 80 }
            },
            Name = new List<Name> {
                    new Name {
                        Url = "http://C"
                    }
                }
        };

        private readonly List<Document> _data;

        private readonly Striper<Document> _striper;

        public StriperTest() {
            _data = new List<Document> { _r1, _r2 };

            _striper = new Striper<Document>(typeof(Document).GetParquetSchema(false));
        }

        [Fact]
        public void Schema_AllLevels() {
            // check schema
            ParquetSchema schema = typeof(Document).GetParquetSchema(false);

            // DocId
            Assert.Equal(0, schema[0].MaxRepetitionLevel);
            Assert.Equal(0, schema[0].MaxDefinitionLevel);

            // Links
            Assert.Equal(0, schema[1].MaxRepetitionLevel);
            Assert.Equal(1, schema[1].MaxDefinitionLevel);

            // Links.Backward
            Assert.Equal(1, schema[1].Children[0].MaxRepetitionLevel);
            Assert.Equal(2, schema[1].Children[0].MaxDefinitionLevel);

            // Links.Forward
            Assert.Equal(1, schema[1].Children[1].MaxRepetitionLevel);
            Assert.Equal(2, schema[1].Children[1].MaxDefinitionLevel);

            // Name.Language.Code
            Field nlCode = schema[2].NaturalChildren[0].NaturalChildren[0];
            Assert.Equal("Name.list.element.Language.list.element.Code", nlCode.Path);
            Assert.Equal(2, nlCode.MaxRepetitionLevel);
            Assert.Equal(3, nlCode.MaxDefinitionLevel);

            // Name.Language.Country
            Field nlCountry = schema[2].NaturalChildren[0].NaturalChildren[1];
            Assert.Equal("Name.list.element.Language.list.element.Country", nlCountry.Path);
            Assert.Equal(2, nlCountry.MaxRepetitionLevel);
            Assert.Equal(3, nlCountry.MaxDefinitionLevel);

            // Name.Url
            Assert.Equal("Name.list.element.Url", schema[2].Children[0].Children[1].Path);
            Assert.Equal(1, schema[2].Children[0].Children[1].MaxRepetitionLevel);
            Assert.Equal(2, schema[2].Children[0].Children[1].MaxDefinitionLevel);
        }


        [Fact]
        public void Totals_Has6Stripers() {

            Assert.Equal(6, _striper.FieldStripers.Count);
        }

        [Fact]
        public void Field_1_DocId() {
            // DocId (field 1 of 6)
            FieldStriper<Document> striper = _striper.FieldStripers[0];
            ShreddedColumn col = striper.Stripe(striper.Field, _data);
            Assert.Equal(new long[] { 10, 20 }, col.Data);
            Assert.Null(col.RepetitionLevels);
            Assert.Null(col.DefinitionLevels);
        }

        [Fact]
        public void Field_2_Links_Backward() {

            // Links.Backward (field 2 of 6)
            FieldStriper<Document> striper = _striper.FieldStripers[1];
            ShreddedColumn col = striper.Stripe(striper.Field, _data);
            Assert.Equal(new long[] { 10, 30 }, col.Data);
            Assert.Equal(new int[] { 0, 0, 1 }, col.RepetitionLevels!);
            Assert.Equal(new int[] { 1, 2, 2 }, col.DefinitionLevels!);
        }

        // Links.Forward (field 3 of 6)
        [Fact]
        public void Field_3_Links_Forward() {
            FieldStriper<Document> striper = _striper.FieldStripers[2];
            ShreddedColumn col = striper.Stripe(striper.Field, _data);
            Assert.Equal(new long[] { 20, 40, 60, 80 }, col.Data);
            Assert.Equal(new int[] { 0, 1, 1, 0 }, col.RepetitionLevels!);
            Assert.Equal(new int[] { 2, 2, 2, 2 }, col.DefinitionLevels!);
        }

        [Fact]
        public void Field_4_Name_Language_Code() {
            FieldStriper<Document> striper = _striper.FieldStripers[3];
            ShreddedColumn col = striper.Stripe(striper.Field, _data);
            Assert.Equal(new string[] { "en-us", "en", "en-gb" }, col.Data);
            Assert.Equal(new int[] { 0, 2, 1, 1, 0 }, col.RepetitionLevels!);
            Assert.Equal(new int[] { 3, 3, 1, 3, 1 }, col.DefinitionLevels!);
        }

        [Fact]
        public void Field_5_Name_Language_Country() {
            FieldStriper<Document> striper = _striper.FieldStripers[4];
            ShreddedColumn col = striper.Stripe(striper.Field, _data);
            Assert.Equal(new string[] { "us", "gb" }, col.Data);
            Assert.Equal(new int[] { 0, 2, 1, 1, 0 }, col.RepetitionLevels!);
            Assert.Equal(new int[] { 3, 2, 1, 3, 1 }, col.DefinitionLevels!);
        }

        [Fact]
        public void Field_6_Name_Url() {
            FieldStriper<Document> striper = _striper.FieldStripers[5];
            ShreddedColumn col = striper.Stripe(striper.Field, _data);
            Assert.Equal(new string[] { "http://A", "http://B", "http://C" }, col.Data);
            Assert.Equal(new int[] { 0, 1, 1, 0 }, col.RepetitionLevels!);
            Assert.Equal(new int[] { 2, 2, 1, 2 }, col.DefinitionLevels!);
        }
    }
}

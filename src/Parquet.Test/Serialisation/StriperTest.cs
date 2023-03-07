using System.Collections.Generic;
using Parquet.Schema;
using Parquet.Serialization;
using Parquet.Serialization.Dremel;
using Parquet.Test.Serialisation.Paper;
using Xunit;

namespace Parquet.Test.Serialisation {
    public class StriperTest {

        [Fact]
        public void DocumentSchemaLevelsTest() {
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

        /// <summary>
        /// This test validates repetition and definition levels are generated correctly according to the main
        /// Dremel Paper written by google. 
        /// Link to original paper: https://research.google/pubs/pub36632/
        /// </summary>
        [Fact]
        public void StripeR1R2Test() {
            var striper = new Striper<Document>(typeof(Document).GetParquetSchema(false));

            Assert.Equal(6, striper.FieldStripers.Count);

            var r1 = new Document {
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

            var r2 = new Document {
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

            var data = new List<Document> { r1, r2 };

            // DocId (field 1 of 6)
            FieldStriper<Document> docIdStriper = striper.FieldStripers[0];
            ShreddedColumn docIdColumn = docIdStriper.Stripe(docIdStriper.Field, data);
            Assert.Equal(docIdColumn.Data, new long[] { 10, 20 });
            Assert.Null(docIdColumn.RepetitionLevels);
            Assert.Null(docIdColumn.DefinitionLevels);

            // Links.Backward (field 2 of 6)
            FieldStriper<Document> backwardStriper = striper.FieldStripers[1];
            ShreddedColumn backwardColumn = backwardStriper.Stripe(backwardStriper.Field, data);
            Assert.Equal(backwardColumn.Data, new long[] { 10, 30 });
            Assert.Equal(backwardColumn.RepetitionLevels!, new int[] { 0, 0, 1 });
            Assert.Equal(backwardColumn.DefinitionLevels!, new int[] { 1, 2, 2 });

            // Links.Forward (field 3 of 6)
            FieldStriper<Document> forwardStriper = striper.FieldStripers[2];
            ShreddedColumn forwardColumn = forwardStriper.Stripe(forwardStriper.Field, data);
            Assert.Equal(forwardColumn.Data, new long[] { 20, 40, 60, 80 });
            Assert.Equal(forwardColumn.RepetitionLevels!, new int[] { 0, 1, 1, 0 });
            Assert.Equal(forwardColumn.DefinitionLevels!, new int[] { 2, 2, 2, 2 });


            //Name.Url (field 2 of 6)
            //FieldStriper<Document> urlStriper = striper.FieldStripers[1];
            //ShreddedColumn urlColumn = urlStriper.Stripe(urlStriper.Field, data);
            //Assert.Equal(urlColumn.Data, new string[] { "http://A", "http://B", "http://C" });
            //Assert.Equal(urlColumn.RepetitionLevels!, new int[] { 0, 1, 1, 0 });
            //Assert.Equal(urlColumn.DefinitionLevels!, new int[] { 2, 2, 1, 2 });


        }
    }
}

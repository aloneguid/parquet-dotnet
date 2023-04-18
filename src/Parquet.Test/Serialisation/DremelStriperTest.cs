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
    public class DremelStriperTest {

        private readonly Striper<Document> _striper;

        public DremelStriperTest() {
            _striper = new Striper<Document>(typeof(Document).GetParquetSchema(false));
        }

        [Fact]
        public void Schema_AllLevels() {
            // check schema
            ParquetSchema schema = typeof(Document).GetParquetSchema(false);

            // DocId
            Field docId = schema[0];
            Assert.Equal(new FieldPath("DocId"), docId.Path);
            Assert.Equal(0, docId.MaxRepetitionLevel);
            Assert.Equal(0, docId.MaxDefinitionLevel);

            // Links
            Field links = schema[1];
            Assert.Equal(new FieldPath("Links"), links.Path);
            Assert.Equal(0, links.MaxRepetitionLevel);
            Assert.Equal(1, links.MaxDefinitionLevel);

            // Links.Backward
            Field lBack = schema[1].Children[0];
            Assert.Equal(new FieldPath("Links", "Backward", "list"), lBack.Path);
            Assert.Equal(1, lBack.MaxRepetitionLevel);
            Assert.Equal(3, lBack.MaxDefinitionLevel);

            // Links.Forward
            Field lForw = schema[1].Children[1];
            Assert.Equal(new FieldPath("Links", "Forward", "list"), lForw.Path);
            Assert.Equal(1, lForw.MaxRepetitionLevel);
            Assert.Equal(3, lForw.MaxDefinitionLevel);

            // Name.Language.Code
            Field nlCode = schema[2].NaturalChildren[0].NaturalChildren[0];
            Assert.Equal(new FieldPath("Name", "list", "element", "Language", "list", "element", "Code"), nlCode.Path);
            Assert.Equal(2, nlCode.MaxRepetitionLevel);
            Assert.Equal(7, nlCode.MaxDefinitionLevel);

            // Name.Language.Country
            Field nlCountry = schema[2].NaturalChildren[0].NaturalChildren[1];
            Assert.Equal(new FieldPath("Name", "list", "element", "Language", "list", "element", "Country"), nlCountry.Path);
            Assert.Equal(2, nlCountry.MaxRepetitionLevel);
            Assert.Equal(7, nlCountry.MaxDefinitionLevel);

            // Name.Url
            Assert.Equal(new FieldPath("Name", "list", "element", "Url"), schema[2].Children[0].Children[1].Path);
            Assert.Equal(1, schema[2].Children[0].Children[1].MaxRepetitionLevel);
            Assert.Equal(4, schema[2].Children[0].Children[1].MaxDefinitionLevel);
        }


        [Fact]
        public void Totals_Has6Stripers() {

            Assert.Equal(6, _striper.FieldStripers.Count);
        }

        [Fact]
        public void Field_1_DocId() {
            // DocId (field 1 of 6)
            FieldStriper<Document> striper = _striper.FieldStripers[0];
            ShreddedColumn col = striper.Stripe(striper.Field, Document.Both);
            Assert.Equal(new long[] { 10, 20 }, col.Data);
            Assert.Null(col.RepetitionLevels);
            Assert.Null(col.DefinitionLevels);
        }

        [Fact]
        public void Field_2_Links_Backward() {

            // Links.Backward (field 2 of 6)
            FieldStriper<Document> striper = _striper.FieldStripers[1];
            ShreddedColumn col = striper.Stripe(striper.Field, Document.Both);
            Assert.Equal(new long[] { 10, 30 }, col.Data);
            Assert.Equal(new int[] { 0, 0, 1 }, col.RepetitionLevels!);
            Assert.Equal(new int[] { 1, 3, 3 }, col.DefinitionLevels!);
        }

        // Links.Forward (field 3 of 6)
        [Fact]
        public void Field_3_Links_Forward() {
            FieldStriper<Document> striper = _striper.FieldStripers[2];
            ShreddedColumn col = striper.Stripe(striper.Field, Document.Both);
            Assert.Equal(new long[] { 20, 40, 60, 80 }, col.Data);
            Assert.Equal(new int[] { 0, 1, 1, 0 }, col.RepetitionLevels!);
            Assert.Equal(new int[] { 3, 3, 3, 3 }, col.DefinitionLevels!);
        }

        [Fact]
        public void Field_4_Name_Language_Code() {
            FieldStriper<Document> striper = _striper.FieldStripers[3];
            ShreddedColumn col = striper.Stripe(striper.Field, Document.Both);
            Assert.Equal(new string[] { "en-us", "en", "en-gb" }, col.Data);
            Assert.Equal(new int[] { 0, 2, 1, 1, 0 }, col.RepetitionLevels!);
            Assert.Equal(new int[] { 7, 7, 3, 7, 3 }, col.DefinitionLevels!);
        }

        [Fact]
        public void Field_5_Name_Language_Country() {
            FieldStriper<Document> striper = _striper.FieldStripers[4];
            ShreddedColumn col = striper.Stripe(striper.Field, Document.Both);
            Assert.Equal(new string[] { "us", "gb" }, col.Data);
            Assert.Equal(new int[] { 0, 2, 1, 1, 0 }, col.RepetitionLevels!);
            Assert.Equal(new int[] { 7, 6, 3, 7, 3 }, col.DefinitionLevels!);
        }

        [Fact]
        public void Field_6_Name_Url() {
            FieldStriper<Document> striper = _striper.FieldStripers[5];
            ShreddedColumn col = striper.Stripe(striper.Field, Document.Both);
            Assert.Equal(new string[] { "http://A", "http://B", "http://C" }, col.Data);
            Assert.Equal(new int[] { 0, 1, 1, 0 }, col.RepetitionLevels!);
            Assert.Equal(new int[] { 4, 4, 3, 4 }, col.DefinitionLevels!);
        }
    }
}

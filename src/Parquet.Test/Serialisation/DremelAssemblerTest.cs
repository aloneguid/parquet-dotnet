using System;
using System.Collections.Generic;
using System.Text.Json;
using Parquet.Serialization;
using Parquet.Serialization.Dremel;
using Parquet.Test.Serialisation.Paper;
using Parquet.Test.Xunit;
using Xunit;

namespace Parquet.Test.Serialisation {
    public class DremelAssemblerTest {

        private readonly Assembler<Document> _asm;

        public DremelAssemblerTest() {
            _asm = new Assembler<Document>(typeof(Document).GetParquetSchema(true));
        }

        [Fact]
        public void Totals_Has6Assemblers() {
            Assert.Equal(6, _asm.FieldAssemblers.Count);
        }

        [Fact]
        public void Field_1_DocId() {
            var docs = new List<Document> { new Document(), new Document() };
            _asm.FieldAssemblers[0].Assemble(docs, Document.RawColumns[0]);
            Assert.Equal(10, docs[0].DocId);
            Assert.Equal(20, docs[1].DocId);
        }

        [Fact]
        public void Field_2_Links_Backward() {
            var docs = new List<Document> { new Document(), new Document() };
            _asm.FieldAssemblers[1].Assemble(docs, Document.RawColumns[1]);
            Assert.NotNull(docs[0].Links);
            Assert.Null(docs[0].Links!.Backward);
            Assert.Null(docs[0].Links!.Forward);


            Assert.NotNull(docs[1].Links);
            Assert.Equal(new long[] { 10, 30 }, docs[1].Links!.Backward!);
            Assert.Null(docs[1].Links!.Forward);
        }

        [Fact]
        public void Field_3_Links_Forward() {
            var docs = new List<Document> { new Document(), new Document() };
            _asm.FieldAssemblers[2].Assemble(docs, Document.RawColumns[2]);
            Assert.NotNull(docs[0].Links);
            Assert.NotNull(docs[1].Links);

            Assert.Equal(new long[] { 20, 40, 60 }, docs[0].Links!.Forward!);
            Assert.Equal(new long[] { 80 },         docs[1].Links!.Forward!);
        }

        [Fact]
        public void Field_4_Name_Language_Code() {
            var docs = new List<Document> { new Document(), new Document() };
            _asm.FieldAssemblers[3].Assemble(docs, Document.RawColumns[3]);

            // assert

            // Name
            Assert.NotNull(docs[0].Name);
            Assert.NotNull(docs[1].Name);

            // Name count is 3, regardless of the null value
            Assert.Equal(3, docs[0].Name!.Count);
            Assert.Single(docs[1].Name!);

            // Language count
            Assert.Equal(2, docs[0].Name![0].Language!.Count);

            // language values
            Assert.Equal("en-us",   docs[0].Name![0].Language![0].Code);
            Assert.Equal("en",      docs[0].Name![0].Language![1].Code);
            Assert.Equal("en-gb",   docs[0].Name![2].Language![0].Code);
        }

        [Fact]
        public void Field_5_Name_Language_Country() {
            var docs = new List<Document> { new Document(), new Document() };
            _asm.FieldAssemblers[4].Assemble(docs, Document.RawColumns[4]);

            // assert

            // Name
            Assert.NotNull(docs[0].Name);
            Assert.NotNull(docs[1].Name);

            Assert.Equal(3, docs[0].Name!.Count);

            // Language count
            Assert.Equal(2, docs[0].Name![0].Language!.Count);

            // language values
            Assert.Equal("us", docs[0].Name![0].Language![0].Country);
            Assert.Null(docs[0].Name![0].Language![1].Country);
            Assert.Equal("gb", docs[0].Name![2].Language![0].Country);
        }

        [Fact]
        public void Field_6_Name_Url() {
            var docs = new List<Document> { new Document(), new Document() };
            _asm.FieldAssemblers[5].Assemble(docs, Document.RawColumns[5]);

            // assert

            // Name
            Assert.NotNull(docs[0].Name);
            Assert.NotNull(docs[1].Name);

            Assert.Equal(3, docs[0].Name!.Count);
            Assert.Single(docs[1].Name!);
        }

        [Fact]
        public void FullReassembly() {
            var docs = new List<Document> {  new Document(), new Document() };

            for(int i = 0; i < Document.RawColumns.Length; i++) {
                try {
                    _asm.FieldAssemblers[i].Assemble(docs, Document.RawColumns[i]);
                } catch(Exception ex) {
                    throw new InvalidOperationException("failure on " + _asm.FieldAssemblers[i].Field, ex); 
                }
            }

            XAssert.JsonEquivalent(Document.Both, docs);
        }
    }
}

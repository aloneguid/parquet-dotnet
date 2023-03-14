using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Test.Serialisation.Paper {

    class Document {
        public long DocId { get; set; }

        public Links? Links { get; set; }

        public List<Name>? Name { get; set; }

        public static Document R1 => new() {
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

        public static Document R2 => new() {
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

        public static List<Document> Both => new List<Document>{ R1, R2 };

        public static DataColumn[] RawColumns {
            get {
                return new DataColumn[] {
                    new DataColumn(new DataField<long>("DocId"),
                        new long[] { 10, 20 },
                        null,
                        null,
                        false),

                    new DataColumn(new DataField<long>("Backward"),
                        new long[] { 10, 30 },
                        new() { 1, 2, 2 },
                        new() { 0, 0, 1 },
                        false),

                    new DataColumn(new DataField<long>("Forward"),
                        new long[] { 20, 40, 60, 80 },
                        new() { 2, 2, 2, 2 },
                        new() { 0, 1, 1, 0 },
                        false),

                    new DataColumn(new DataField<string>("Code"),
                        new string[] { "en-us", "en", "en-gb" },
                        new() { 3, 3, 1, 3, 1 },
                        new() { 0, 2, 1, 1, 0 },
                        false),

                    new DataColumn(new DataField<string>("Country"),
                        new string[] { "us", "gb" },
                        new() { 3, 2, 1, 3, 1 },
                        new() { 0, 2, 1, 1, 0 },
                        false),

                    new DataColumn(new DataField<string>("Url"),
                        new string[] { "http://A", "http://B", "http://C" },
                        new() { 2, 2, 1, 2 },
                        new() { 0, 1, 1, 0 },
                        false)
                };
            }
        }
    }
}

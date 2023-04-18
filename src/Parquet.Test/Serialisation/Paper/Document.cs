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

        public static List<Document> Both => new List<Document> { R1, R2 };

        public static DataColumn[] RawColumns {
            get {

                var schema = new ParquetSchema(

                    new DataField<long>("DocId"),

                    new StructField("Links",
                        new ListField("Backward", new DataField<long>("element")),
                        new ListField("Forward", new DataField<long>("element"))),

                    new ListField("Name",
                        new StructField("element",
                            new ListField("Language",
                                new StructField("element", 
                                    new DataField<string>("Code"),
                                    new DataField<string>("Country"))),

                            new DataField<string>("Url")
                        )));

                DataField[] dfs = schema.GetDataFields();

                return new DataColumn[] {
                    new DataColumn(dfs[0], new long[] { 10, 20 }),

                    // Links/Backwards
                    new DataColumn(dfs[1],
                        new long[] { 10, 30 },
                        new int[] { 1, 3, 3 },
                        new int[] { 0, 0, 1 }),

                    // Links/Forwards
                    new DataColumn(dfs[2],
                        new long[] { 20, 40, 60, 80 },
                        new int[] { 3, 3, 3, 3 },
                        new int[] { 0, 1, 1, 0 }),

                    // Code
                    new DataColumn(dfs[3],
                        new string[] { "en-us", "en", "en-gb" },
                        new int[] { 7, 7, 2, 7, 2 },
                        new int[] { 0, 2, 1, 1, 0 }),

                    // Country
                    new DataColumn(dfs[4],
                        new string[] { "us", "gb" },
                        new int[] { 7, 6, 2, 7, 2 },
                        new int[] { 0, 2, 1, 1, 0 }),

                    // Url
                    new DataColumn(dfs[5],
                        new string[] { "http://A", "http://B", "http://C" },
                        new int[] { 4, 4, 3, 4 },
                        new int[] { 0, 1, 1, 0 })
                };
            }
        }
    }
}

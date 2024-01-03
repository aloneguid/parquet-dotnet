using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Parquet.Floor.ViewModels {

#if DEBUG

    using Parquet.Schema;

    static class DesignData {
        public static ParquetSchema Schema { get; } =
            new ParquetSchema(
                new DataField<int>("id"),
                new DataField<string>("name"),
                new StructField("address",
                    new DataField<string>("street"),
                            new DataField<string>("city"),
                            new DataField<string>("state"),
                            new DataField<string>("zip"),
                            new DataField<string>("phone"),
                            new DataField<string>("email"),
                            new DataField<string>("web")));

        public static IList<Dictionary<string, object>> Data { get; } =
            new List<Dictionary<string, object>> {
            new Dictionary<string, object> {
                ["id"] = 1,
                ["name"] = "John Doe",
                ["address"] = new Dictionary<string, object> {
                    ["street"] = "123 Main St",
                    ["city"] = "Anytown",
                    ["state"] = "CA",
                    ["zip"] = "12345",
                    ["phone"] = "555-555-5555",
                    ["email"] = ""
                }
            }
        };
    }

#endif
}

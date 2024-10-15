using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Serialization;
using Parquet.Serialization.Attributes;
using Xunit;

namespace Parquet.Test.Types {
    public class LogicalTimestampTest {
        internal class V1 {
            public DateTime DateTimeUtc { get; set; }

            public DateTime DateTimeLocal { get; set; }
        }

        internal class V2 {
            [ParquetTimestamp(useLogicalTimestamp: true, isAdjustedToUTC: true)]

            public DateTime DateTimeUtc { get; set; }

            [ParquetTimestamp(useLogicalTimestamp: true, isAdjustedToUTC: false)]
            public DateTime DateTimeLocal { get; set; }
        }

        [Fact]
        public async Task SerializeDeserializeV1() {
            var utc = new DateTime(2006, 1, 2, 15, 4, 5, DateTimeKind.Utc);
            // TODO: should this be unspecified?
            var local = new DateTime(2006, 1, 2, 15, 4, 5, DateTimeKind.Local);

            var expected = new V1 {
                DateTimeUtc = utc,
                DateTimeLocal = local,
            };

            using var ms = new MemoryStream();
            await ParquetSerializer.SerializeAsync([expected], ms);

            ms.Position = 0;
            IList<V1> actuals = await ParquetSerializer.DeserializeAsync<V1>(ms);
            Assert.Single(actuals);
            V1 actual = actuals.First();
            Assert.Equal(utc, actual.DateTimeUtc);
            Assert.Equal(DateTimeKind.Utc, actual.DateTimeUtc.Kind);
            // Kind is changed from Local to UTC
            Assert.Equal(local, actual.DateTimeUtc);
            Assert.Equal(DateTimeKind.Utc, actual.DateTimeUtc.Kind);
        }
        
        [Fact]
        public async Task SerializeDeserializeLogicalTimestamp() {
            var utc = new DateTime(2006, 1, 2, 15, 4, 5, DateTimeKind.Utc);
            // TODO: should this be unspecified?
            var local = new DateTime(2006, 1, 2, 15, 4, 5, DateTimeKind.Local);

            var expected = new V2 {
                DateTimeUtc = utc,
                DateTimeLocal = local,
            };

            using var ms = new MemoryStream();
            await ParquetSerializer.SerializeAsync([expected], ms);

            ms.Position = 0;
            IList<V2> actuals = await ParquetSerializer.DeserializeAsync<V2>(ms);
            Assert.Single(actuals);
            V2 actual = actuals.First();
            Assert.Equal(utc, actual.DateTimeUtc);
            Assert.Equal(DateTimeKind.Utc, actual.DateTimeUtc.Kind);
            Assert.Equal(local, actual.DateTimeLocal);
            Assert.Equal(DateTimeKind.Local, actual.DateTimeLocal.Kind);
        }
    }
}
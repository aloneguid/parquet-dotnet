using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using Parquet.File.Values.Primitives;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Schema;

namespace Parquet.Test.Types {
    public static class TypeExtensions {
        public static bool IsArrayOf<T>(this Type type) {
            return type == typeof(T[]);
        }
    }

    public class EndToEndTypeTest : TestBase {

        [Theory, TestBase.TypeTestData]
        public async Task Type_writes_and_reads_end_to_end(TTI input) {

            object actual = await WriteReadSingle(input.Field, input.ExpectedValue);

            bool equal;
            if(input.ExpectedValue == null && actual == null)
                equal = true;
            else if(actual.GetType().IsArrayOf<byte>() && input.ExpectedValue != null) {
                equal = ((byte[])actual).SequenceEqual((byte[])input.ExpectedValue);
            } else if(input.Field is DateTimeDataField { DateTimeFormat: DateTimeFormat.Timestamp }) {
                var dtActual = (DateTime)actual;
                var dtExpected = (DateTime)input.ExpectedValue!;
                Assert.Equal(dtExpected.Kind, dtActual.Kind);
                equal = dtActual.Equals(dtExpected);
            } else if(actual.GetType() == typeof(DateTime)) {
                var dtActual = (DateTime)actual;
                var dtExpected = (DateTime)input.ExpectedValue!;
                dtExpected = dtExpected.Kind == DateTimeKind.Unspecified
                    ? DateTime.SpecifyKind(dtExpected, DateTimeKind.Utc) // assumes value is UTC
                    : dtExpected.ToUniversalTime();
                bool isInt96 = input.Field is not DateTimeDataField ||
                    input.Field is DateTimeDataField { DateTimeFormat: DateTimeFormat.Impala };
                if(isInt96) {
                    Assert.Equal(DateTimeKind.Unspecified, dtActual.Kind);
                    dtExpected = DateTime.SpecifyKind(dtExpected, DateTimeKind.Unspecified);
                } else {
                    Assert.Equal(DateTimeKind.Utc, dtActual.Kind);
                }
                equal = dtActual.Equals(dtExpected);
            } else {
                equal = actual.Equals(input.ExpectedValue);
            }

            Assert.True(equal, $"{input.Name}| expected: [{input.ExpectedValue}], actual: [{actual}], schema element: {input.Field}");
        }

    }
}

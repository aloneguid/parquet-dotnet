using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Xunit.Sdk;

namespace Parquet.Test.Xunit;

public class RepeatAttribute : DataAttribute {
    private readonly int _count;

    public RepeatAttribute(int count) {
        if(count < 1) {
            throw new ArgumentOutOfRangeException(nameof(count),
                "Repeat count must be greater than 0.");
        }
        _count = count;
    }

    public override IEnumerable<object[]> GetData(MethodInfo testMethod) {
        return Enumerable.Range(0, _count).Select(i => new object[] { i });
    }
}
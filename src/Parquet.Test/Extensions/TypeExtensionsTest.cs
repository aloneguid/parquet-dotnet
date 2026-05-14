using System;
using System.Collections.Generic;
using Xunit;

namespace Parquet.Test.Extensions;

public class TypeExtensionsTest {
    [Fact]
    public void String_array_is_enumerable() {
        Assert.True(typeof(string[]).TryExtractIEnumerableType(out Type? et));
        Assert.Equal(typeof(string), et);
    }

    [Fact]
    public void String_is_not_enumerable() {
        Assert.False(typeof(string).TryExtractIEnumerableType(out Type? et));
    }

    [Fact]
    public void StringIenumerable_is_enumerable() {
        Assert.True(typeof(IEnumerable<string>).TryExtractIEnumerableType(out Type? et));
        Assert.Equal(typeof(string), et);
    }

    [Fact]
    public void Nullable_element_is_not_stripped() {
        Assert.True(typeof(IEnumerable<int?>).TryExtractIEnumerableType(out Type? et));
        Assert.Equal(typeof(int?), et);
    }

    [Fact]
    public void ListOfT_is_ienumerable() {
        Assert.True(typeof(List<int>).TryExtractIEnumerableType(out Type? baseType));
        Assert.Equal(typeof(int), baseType);
    }
}
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Schema;

public class FieldPathTest {
    [Fact]
    public void Construct_from_perfect_string() {
        var p = new FieldPath("ID");

        Assert.Single(p.ToList());
        Assert.Equal("ID", p.ToList()[0]);
    }

    [Fact]
    public void Construct_from_name_with_dot_inside() {
        var p = new FieldPath("ID.");

        Assert.Single(p.ToList());
        Assert.Equal("ID.", p.ToList()[0]);
    }

    [Fact]
    public void Equal_simple_same() {
        Assert.Equal(new FieldPath("id"), new FieldPath("id"));
    }

    [Fact]
    public void Equal_simple_not_same() {
        Assert.NotEqual(new FieldPath("id1"), new FieldPath("id"));
    }
}
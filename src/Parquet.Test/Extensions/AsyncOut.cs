namespace Parquet.Test.Extensions;

/// <summary>
/// Simulates OUT parameters in async methods.
/// </summary>
/// <typeparam name="T"></typeparam>
class AsyncOut<T> {
    public T? Value { get; set; }
}
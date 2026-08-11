using System;
using System.Collections.Generic;
using System.Text;

namespace Parquet.Meta.Proto;

/// <summary>
/// Apache thrift protocol related exception
/// </summary>
public class ThriftProtocolException : Exception {

    /// <summary>
    /// Initializes a new instance of the ThriftProtocolException class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public ThriftProtocolException(string message) : base(message) { }

    /// <summary>
    /// Initializes a new instance of the ThriftProtocolException class with a specified error message and a reference
    /// to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception, or a null reference if no inner exception is
    /// specified.</param>
    public ThriftProtocolException(string message, Exception innerException) : base(message, innerException) { }
}
    
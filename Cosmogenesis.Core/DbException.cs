using System.Runtime.Serialization;

namespace Cosmogenesis.Core;

public abstract class DbException : Exception
{
    internal DbException()
    {
    }

    internal DbException(string message) : base(message)
    {
    }

    internal DbException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

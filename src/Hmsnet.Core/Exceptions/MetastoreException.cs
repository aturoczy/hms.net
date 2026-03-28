namespace Hmsnet.Core.Exceptions;

public class MetastoreException : Exception
{
    public MetastoreException(string message) : base(message) { }
    public MetastoreException(string message, Exception inner) : base(message, inner) { }
}

public class NoSuchObjectException : MetastoreException
{
    public NoSuchObjectException(string message) : base(message) { }
}

public class AlreadyExistsException : MetastoreException
{
    public AlreadyExistsException(string message) : base(message) { }
}

public class InvalidOperationException : MetastoreException
{
    public InvalidOperationException(string message) : base(message) { }
}

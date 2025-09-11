namespace Cosmogenesis.Core;

public sealed class CreateResult<T> where T : DbDoc
{
    internal static readonly CreateResult<T> AlreadyExists = new(DbConflictType.AlreadyExists);

    internal CreateResult(DbConflictType conflict)
    {
        if (conflict != DbConflictType.AlreadyExists)
        {
            throw new ArgumentOutOfRangeException(nameof(conflict));
        }

        Conflict = conflict;
    }
    internal CreateResult(T document)
    {
        ArgumentNullException.ThrowIfNull(document);
        Document = document;
    }
    public T? Document { get; }
    public DbConflictType? Conflict { get; }
}

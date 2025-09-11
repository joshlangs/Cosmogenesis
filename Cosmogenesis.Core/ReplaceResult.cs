namespace Cosmogenesis.Core;

public sealed class ReplaceResult<T> where T : DbDoc
{
    internal static readonly ReplaceResult<T> ETagChanged = new(DbConflictType.ETagChanged);
    internal static readonly ReplaceResult<T> Missing = new(DbConflictType.Missing);

    internal ReplaceResult(DbConflictType conflict)
    {
        if (conflict != DbConflictType.ETagChanged &&
            conflict != DbConflictType.Missing)
        {
            throw new ArgumentOutOfRangeException(nameof(conflict));
        }

        Conflict = conflict;
    }
    internal ReplaceResult(T document)
    {
        ArgumentNullException.ThrowIfNull(document);
        Document = document;
    }
    public T? Document { get; }
    public DbConflictType? Conflict { get; }
}

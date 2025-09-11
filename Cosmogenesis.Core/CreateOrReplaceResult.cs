namespace Cosmogenesis.Core;

public sealed class CreateOrReplaceResult<T> where T : DbDoc
{
    /// <summary>
    /// The document that was created or replaced in the database
    /// </summary>
    public T Document { get; }

    /// <summary>
    /// True if the document was replaced (already existed).
    /// False if the document was created (did not exist).
    /// </summary>
    public bool AlreadyExisted { get; }

    internal CreateOrReplaceResult(T document, bool alreadyExisted)
    {
        ArgumentNullException.ThrowIfNull(document);
        Document = document;
        AlreadyExisted = alreadyExisted;
    }
}

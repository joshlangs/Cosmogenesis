using System.Text.Json.Serialization;

namespace Cosmogenesis.Core;

public abstract class DbDoc
{
    const int MaxPartitionKeyLength = 2048;
    const int MaxIdLength = 1023;

    // These don't have friendly naming because we run into tricky issues.
    // We could apply JsonPropertyName attributes for example, but then
    // the Linq provider would generate the wrong names

#pragma warning disable IDE1006 // Naming Styles
    /// <summary>
    /// Partition key.
    /// System property - do not set manually.
    /// </summary>
    public string pk { get; set; } = default!;
    /// <summary>
    /// Document ID.
    /// Unique within a partition.
    /// System property - do not set manually.
    /// </summary>
    public string id { get; set; } = default!;
    /// <summary>
    /// ETag.
    /// Changes every time a document is updated.  Used for optimistic concurrency.
    /// System property - do not set manually.
    /// </summary>
    public string _etag { get; init; } = default!;
    /// <summary>
    /// *Approx* time of last document update.  Unix epoch (# seconds since Jan 1 1970).
    /// This may not match exactly what's saved in the database at all times.  It's *approximately* correct.
    /// System property - do not set manually.
    /// </summary>
    public int _ts { get; init; }
    /// <summary>
    /// The time (in seconds) until a document can be automatically deleted, if the container has enabled auto-expiry.
    /// System property - do not set manually.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public int? ttl { get; set; }
#pragma warning restore IDE1006 // Naming Styles

    string type = default!;
    /// <summary>
    /// Identifies the type of document.  Used internally for querying and serialization.
    /// Internal property - do not set manually.
    /// </summary>    
    [JsonPropertyOrder(int.MinValue)]
    public string Type
    {
        get => type;
        set
        {
            if (_etag is null || type is null)
            {
                type = value;
            }
            else if (type != value)
            {
                throw new InvalidOperationException($"{nameof(Type)} should not be changed once a document is created");
            }
        }
    }

    DateTime creationDate = IsoDateCheater.MinValue;
    /// <summary>
    /// The document's creation date.  This value is set in the client when a document is created
    /// (as opposed to being set automatically when saved by cosmosdb).  An unconditional replace
    /// operation (like CreateOrReplace) might overwrite this value.
    /// Internal property - do not set manually.
    /// </summary>
    public DateTime CreationDate
    {
        get => creationDate;
        set
        {
            if (_etag is null || creationDate == IsoDateCheater.MinValue)
            {
                creationDate = value;
            }
            else if (creationDate != value)
            {
                throw new InvalidOperationException($"{nameof(CreationDate)} should not be changed once a document is created");
            }
        }
    }

    protected virtual bool ValidateState() =>
        !string.IsNullOrEmpty(pk) &&
        !string.IsNullOrEmpty(id) &&
        pk.Length <= MaxPartitionKeyLength &&
        id.Length <= MaxIdLength &&
        CreationDate != IsoDateCheater.MinValue &&
        !string.IsNullOrEmpty(Type);

    internal void ValidateStateOrThrow()
    {
        if (!ValidateState())
        {
            throw new InvalidOperationException($"The {GetType().FullName} document is in an invalid state");
        }
    }
}

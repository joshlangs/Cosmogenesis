using System.Diagnostics;
using Microsoft.Azure.Cosmos;

namespace Cosmogenesis.Core;

public abstract class DbBatchBase
{
    public const int MaxBatchItems = 100;
    static readonly TransactionalBatchItemRequestOptions OptionsWithoutContentResponse = new() { EnableContentResponseOnWrite = false };

    protected virtual DbSerializerBase Serializer { get; } = default!;
    protected virtual TransactionalBatch TransactionalBatch { get; } = default!;
    protected string PartitionKey { get; } = default!;

    readonly Lock LockObject = new();
    readonly HashSet<string> IdsInBatch = [];
    readonly List<Func<Stream, DbDoc?>?> DeserializeResults = [];
    bool Executed;
    public virtual bool IsEmpty { get; private set; } = true;

    public virtual bool ValidateStateBeforeSave { get; }
    public virtual bool WithResults { get; }
    readonly TransactionalBatchItemRequestOptions? Options;

    protected DbBatchBase() { }

    protected DbBatchBase(
        DbSerializerBase serializer,
        TransactionalBatch transactionalBatch,
        string partitionKey,
        bool validateStateBeforeSave,
        bool withResults)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(transactionalBatch);
        ArgumentNullException.ThrowIfNull(partitionKey);
        Serializer = serializer;
        TransactionalBatch = transactionalBatch;
        PartitionKey = partitionKey;
        ValidateStateBeforeSave = validateStateBeforeSave;
        WithResults = withResults;
        Options = withResults ? null : OptionsWithoutContentResponse;
    }

    void ThrowIfExecuted()
    {
        if (Executed)
        {
            throw new InvalidOperationException("Batches cannot be changed or re-executed after being executed");
        }
    }

    void ThrowIfFull()
    {
        if (DeserializeResults.Count == MaxBatchItems)
        {
            throw new InvalidOperationException($"Batches cannot contain more than {MaxBatchItems} items");
        }
    }

    void EnsureUniqueId(DbDoc doc)
    {
        if (!IdsInBatch.Add(doc.id))
        {
            throw new InvalidOperationException($"An item with the same Id has already been added to the batch");
        }
    }

    static void EnsureIdExists(DbDoc? item)
    {
        ArgumentNullException.ThrowIfNull(item);
        if (item.id is null)
        {
            throw new InvalidOperationException("The document .id property is missing");
        }
    }

    void SetOrMatchPartitionKey(DbDoc item)
    {
        if (item.pk is null)
        {
            item.pk = PartitionKey;
        }
        else if (item.pk != PartitionKey)
        {
            throw new InvalidOperationException("The document .pk property does not match this partition key");
        }
    }

    static void SetOrMatchType(DbDoc item, string? type)
    {
        ArgumentNullException.ThrowIfNull(type);
        if (item.Type is null)
        {
            item.Type = type;
        }
        else if (item.Type != type)
        {
            throw new InvalidOperationException($"The document .type property does not match what was expected ({type})");
        }
    }

    static void EnsureNoETag(DbDoc item)
    {
        if (item._etag is not null)
        {
            throw new InvalidOperationException("The document already has an etag");
        }
    }

    void EnsureETagAndPartitionKey(DbDoc item)
    {
        if (item._etag is null)
        {
            throw new InvalidOperationException("The document is missing an etag");
        }
        if (item.pk != PartitionKey)
        {
            throw new InvalidOperationException("The document .pk property does not match this partition key");
        }
    }

    static void EnsureMatchType(DbDoc item, string? type)
    {
        ArgumentNullException.ThrowIfNull(type);
        if (item.Type != type)
        {
            throw new InvalidOperationException($"The document .type property does not match what was expected ({type})");
        }
    }

    protected virtual async Task<bool> ExecuteCoreAsync()
    {
        lock (LockObject)
        {
            ThrowIfExecuted();
            Executed = true;
            if (IsEmpty)
            {
                return true;
            }
        }

        using var batchResponse = await TransactionalBatch.ExecuteAsync().ConfigureAwait(false);
        if (batchResponse.IsSuccessStatusCode)
        {
            for (var x = 0; x < batchResponse.Count; x++)
            {
                var response = batchResponse[x];
                if (!response.IsSuccessStatusCode)
                {
                    _ = response.StatusCode.DbChangeFromErrorStatus<DbDoc>();
                    return false;
                }
            }

            return true;
        }

        _ = batchResponse.BatchResultFromErrorStatus();
        return false;
    }

    protected virtual async Task ExecuteOrThrowCoreAsync()
    {
        lock (LockObject)
        {
            ThrowIfExecuted();
            Executed = true;
            if (IsEmpty)
            {
                return;
            }
        }

        using var batchResponse = await TransactionalBatch.ExecuteAsync().ConfigureAwait(false);
        if (batchResponse.IsSuccessStatusCode)
        {
            for (var x = 0; x < batchResponse.Count; x++)
            {
                var response = batchResponse[x];
                if (!response.IsSuccessStatusCode)
                {
                    var change = response.StatusCode.DbChangeFromErrorStatus<DbDoc>();
                    throw new DbConflictException(change.Conflict!.Value);
                }
            }

            return;
        }

        var result = batchResponse.BatchResultFromErrorStatus();
        throw new DbConflictException(result.Conflict!.Value);
    }

    protected virtual async Task<BatchResult> ExecuteWithResultsCoreAsync()
    {
        if (!WithResults)
        {
            throw new InvalidOperationException($"This batch does not support returning results");
        }
        lock (LockObject)
        {
            ThrowIfExecuted();
            Executed = true;
            if (IsEmpty)
            {
                return new BatchResult(0);
            }
        }
        using var batchResponse = await TransactionalBatch.ExecuteAsync().ConfigureAwait(false);
        if (batchResponse.IsSuccessStatusCode)
        {
            var result = new BatchResult(DeserializeResults.Count);
            var docs = result.Documents!;
            for (var x = 0; x < batchResponse.Count; x++)
            {
                var response = batchResponse[x];
                if (response.IsSuccessStatusCode)
                {
                    var deserialize = DeserializeResults[x];
                    if (deserialize is not null && response.ResourceStream is not null)
                    {
                        docs.Add(deserialize(response.ResourceStream));
                    }
                    else
                    {
                        docs.Add(null);
                    }
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
            return result;
        }

        return batchResponse.BatchResultFromErrorStatus();
    }

    protected virtual void CreateOrReplaceCore<T>(T item, string type) where T : DbDoc
    {
        EnsureIdExists(item);
        EnsureNoETag(item);
        SetOrMatchPartitionKey(item);
        SetOrMatchType(item, type);

        Debug.Assert(item.CreationDate == IsoDateCheater.MinValue, "Don't set CreationDate. It is overridden anyway.");
        item.CreationDate = DateTime.UtcNow;

        if (ValidateStateBeforeSave)
        {
            item.ValidateStateOrThrow();
        }

        lock (LockObject)
        {
            ThrowIfExecuted();
            ThrowIfFull();
            EnsureUniqueId(item);
            DeserializeResults.Add(Serializer.FromStream<T>);
            TransactionalBatch.UpsertItemStream(streamPayload: Serializer.ToStream(item), requestOptions: Options);
            IsEmpty = false;
        }
    }

    protected virtual void CreateCore<T>(T item, string type) where T : DbDoc
    {
        EnsureIdExists(item);
        EnsureNoETag(item);
        SetOrMatchPartitionKey(item);
        SetOrMatchType(item, type);

        Debug.Assert(item.CreationDate == IsoDateCheater.MinValue, "Don't set CreationDate. It is overridden anyway.");
        item.CreationDate = DateTime.UtcNow;

        if (ValidateStateBeforeSave)
        {
            item.ValidateStateOrThrow();
        }

        lock (LockObject)
        {
            ThrowIfExecuted();
            ThrowIfFull();
            EnsureUniqueId(item);
            DeserializeResults.Add(Serializer.FromStream<T>);
            TransactionalBatch.CreateItemStream(streamPayload: Serializer.ToStream(item), requestOptions: Options);
            IsEmpty = false;
        }
    }

    protected virtual void ReplaceCore<T>(T item, string type) where T : DbDoc
    {
        EnsureIdExists(item);
        EnsureETagAndPartitionKey(item);
        EnsureMatchType(item, type);

        if (ValidateStateBeforeSave)
        {
            item.ValidateStateOrThrow();
        }

        lock (LockObject)
        {
            ThrowIfExecuted();
            ThrowIfFull();
            EnsureUniqueId(item);
            DeserializeResults.Add(Serializer.FromStream<T>);
            TransactionalBatch.ReplaceItemStream(
                id: item.id,
                streamPayload: Serializer.ToStream(item),
                requestOptions: new TransactionalBatchItemRequestOptions
                {
                    IfMatchEtag = item._etag,
                    EnableContentResponseOnWrite = WithResults
                });
            IsEmpty = false;
        }
    }

    protected virtual void DeleteCore<T>(T item) where T : DbDoc
    {
        EnsureIdExists(item);
        EnsureETagAndPartitionKey(item);

        lock (LockObject)
        {
            ThrowIfExecuted();
            ThrowIfFull();
            EnsureUniqueId(item);
            DeserializeResults.Add(null);
            TransactionalBatch.DeleteItem(
                id: item.id,
                requestOptions: new TransactionalBatchItemRequestOptions
                {
                    IfMatchEtag = item._etag,
                    EnableContentResponseOnWrite = false
                });
            IsEmpty = false;
        }
    }
}

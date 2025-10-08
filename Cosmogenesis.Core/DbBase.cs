using System.Diagnostics;
using System.Net;
using System.Runtime.CompilerServices;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;

namespace Cosmogenesis.Core;

public abstract class DbBase
{
    static readonly ItemRequestOptions OptionsWithoutContentResponse = new() { EnableContentResponseOnWrite = false };

    internal const int ReadIdsQueryThreshhold = 4;
    internal const int MaxIdsPerQuery = 10000;
    internal const int MaxIdLengthPerQuery = 500000;
    internal protected virtual Container Container { get; } = default!;
    internal protected virtual DbSerializerBase Serializer { get; } = default!;

    public virtual bool IsReadOnly { get; }
    public bool ValidateStateBeforeSave { get; }
    bool IsWarm;

    protected DbBase() { }

    protected DbBase(
        Container container,
        DbSerializerBase serializer,
        bool isReadOnly,
        bool validateStateBeforeSave)
    {
        ArgumentNullException.ThrowIfNull(container);
        ArgumentNullException.ThrowIfNull(serializer);
        Container = container;
        Serializer = serializer;
        IsReadOnly = isReadOnly;
        ValidateStateBeforeSave = validateStateBeforeSave;
    }

    public async virtual Task ValidateContainerAsync(CancellationToken cancellationToken = default)
    {
        var query = new QueryDefinition("select * from c where c.id=@containerId")
            .WithParameter("@containerId", Container.Id);
        var containers = Container
            .Database
            .GetContainerQueryIterator<ContainerProperties>(query);
        while (containers.HasMoreResults)
        {
            var prop = await containers.ReadNextAsync(cancellationToken).ConfigureAwait(false);
            var settings = prop.Resource.FirstOrDefault();
            if (settings is null)
            {
                continue;
            }
            if (settings.PartitionKeyDefinitionVersion == PartitionKeyDefinitionVersion.V1)
            {
                throw new InvalidOperationException("The container uses the old partition key system; upgrade to V2");
            }
            if (settings.PartitionKeyPath != "/pk")
            {
                throw new InvalidOperationException($"The container's partition key path should be '/pk' but instead is '{settings.PartitionKeyPath}'");
            }
            ValidateContainerDefaultTtl(settings.DefaultTimeToLive);
            return;
        }
        throw new InvalidOperationException("The container does not yet seem to exist");
    }

    protected abstract void ValidateContainerDefaultTtl(int? ttl);

    public async virtual Task WarmupAsync(CancellationToken cancellationToken = default)
    {
        if (IsWarm)
        {
            return;
        }
        using var result = await Container.ReadItemStreamAsync(
            id: Guid.NewGuid().ToString(),
            partitionKey: new PartitionKey(Guid.NewGuid().ToString()),
            cancellationToken: cancellationToken).ConfigureAwait(false);
        IsWarm = true;
    }

    internal void ThrowIfReadOnly()
    {
        if (IsReadOnly)
        {
            throw new InvalidOperationException($"This database instance is in read-only mode");
        }
    }

    protected virtual async Task<DbDoc?> ReadByIdAsync(string id, PartitionKey partitionKey)
    {
        ArgumentNullException.ThrowIfNull(id);

        using var result = await Container.ReadItemStreamAsync(
            id: id,
            partitionKey: partitionKey).ConfigureAwait(false);
        if (result.IsSuccessStatusCode)
        {
            var item = Serializer.FromStream<DbDoc>(result.Content) ?? throw new DbUnexpectedStateException("FromStream<T> returned null");
            Debug.Assert(item.id == id);
            Debug.Assert(partitionKey.ToString() == new PartitionKey(item.pk).ToString());
            Debug.Assert(!string.IsNullOrEmpty(item._etag));
            Debug.Assert(item._ts > 0);
            return item;
        }
        if (result.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
        throw result.ExceptionFromErrorStatus();
    }

    protected virtual async Task<T?> ReadByIdAsync<T>(string id, PartitionKey partitionKey, string type) where T : DbDoc
    {
        ArgumentNullException.ThrowIfNull(id);
        ArgumentNullException.ThrowIfNull(type);

        var doc = await ReadByIdAsync(id, partitionKey).ConfigureAwait(false);
        if (doc is null)
        {
            return null;
        }
        if (doc.Type != type)
        {
            throw new InvalidOperationException($"This document is of type {doc.Type}, not the expected type {type}");
        }
        return (T)doc;
    }

    protected virtual async Task<DbDoc?[]> ReadByIdsAsync(IEnumerable<string> ids, PartitionKey partitionKey)
    {
        ArgumentNullException.ThrowIfNull(ids);

        var idList = ids.Select(x => x ?? throw new ArgumentNullException(nameof(ids))).ToArray() ?? throw new ArgumentNullException(nameof(ids));
        if (idList.Length == 0)
        {
            return [];
        }
        if (idList.Length <= ReadIdsQueryThreshhold)
        {
            var readTasks = idList.Select(id => ReadByIdAsync(id: id, partitionKey: partitionKey)).ToList();
            await Task.WhenAll(readTasks).ConfigureAwait(false);
            return [.. readTasks.Select(x => x.Result)];
        }

        var results = new DbDoc?[idList.Length];
        var resultMap = idList.Select((id, index) => (id, index)).ToDictionary(x => x.id, x => x.index);

        foreach (var queryIds in idList.Segment(MaxIdsPerQuery))
        {
            var totalLength = 0;
            var queryNowIds = queryIds
                .TakeWhile(x => (totalLength += x.Length) < MaxIdLengthPerQuery)
                .ToList();

            var query = Container.GetItemLinqQueryable<DbDoc>(
                allowSynchronousQueryExecution: false,
                requestOptions: new QueryRequestOptions
                {
                    PartitionKey = partitionKey
                })
                .Where(x => queryNowIds.Contains(x.id));

            await foreach (var item in ExecuteQueryAsync(query))
            {
                Debug.Assert(partitionKey.ToString() == new PartitionKey(item.pk).ToString());
                Debug.Assert(!string.IsNullOrEmpty(item._etag));
                Debug.Assert(item._ts > 0);
                results[resultMap[item.id]] = item;
            }
        }

        return results;
    }
    protected virtual async Task<T?[]> ReadByIdsAsync<T>(IEnumerable<string> ids, PartitionKey partitionKey, string type) where T : DbDoc
    {
        ArgumentNullException.ThrowIfNull(ids);
        ArgumentNullException.ThrowIfNull(type);

        var docs = await ReadByIdsAsync(ids, partitionKey).ConfigureAwait(false);
        var results = new T?[docs.Length];
        for (var x = 0; x < docs.Length; ++x)
        {
            var doc = docs[x];
            if (doc is not null)
            {
                if (doc.Type != type)
                {
                    throw new InvalidOperationException($"This document is of type {doc.Type}, not the expected type {type}");
                }
                results[x] = (T)doc;
            }
        }

        return results;
    }

    void PrepareCreateItem<T>(T item, string type, string partitionKeyString) where T : DbDoc
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(type);
        ArgumentNullException.ThrowIfNull(partitionKeyString);

        ThrowIfReadOnly();

        if (item.id is null)
        {
            throw new InvalidOperationException("The document .id property is missing");
        }
        if (item._etag is not null)
        {
            throw new InvalidOperationException("The document already has an etag");
        }
        if (item.pk is null)
        {
            item.pk = partitionKeyString;
        }
        else if (item.pk != partitionKeyString)
        {
            throw new InvalidOperationException("The document .pk property does not match this partition key");
        }
        if (item.Type is null)
        {
            item.Type = type;
        }
        else if (item.Type != type)
        {
            throw new InvalidOperationException($"The document .Type property does not match what was expected ({type})");
        }
        if (item.ttl.HasValue && (item.ttl.Value < -1 || item.ttl.Value == 0))
        {
            throw new InvalidOperationException($"The document .ttl property must be either -1 or greater than 0, if provided");
        }

        Debug.Assert(item.CreationDate == IsoDateCheater.MinValue, "Don't set CreationDate. It is overridden anyway.");
        item.CreationDate = DateTime.UtcNow;

        if (ValidateStateBeforeSave)
        {
            item.ValidateStateOrThrow();
        }
    }
    internal protected virtual async Task<DbConflictType?> CreateItemWithoutResultAsync<T>(T item, string type, PartitionKey partitionKey, string partitionKeyString) where T : DbDoc
    {
        PrepareCreateItem(item, type, partitionKeyString);
        using var payload = Serializer.ToStream(item);
        using var result = await Container.CreateItemStreamAsync(
            streamPayload: payload,
            partitionKey: partitionKey,
            requestOptions: OptionsWithoutContentResponse).ConfigureAwait(false);
        if (result.IsSuccessStatusCode)
        {
            return null;
        }
        return result.CreateConflictTypeFromErrorStatus();

    }
    internal protected virtual async Task<CreateResult<T>> CreateItemAsync<T>(T item, string type, PartitionKey partitionKey, string partitionKeyString) where T : DbDoc
    {
        PrepareCreateItem(item, type, partitionKeyString);
        using var payload = Serializer.ToStream(item);
        using var result = await Container.CreateItemStreamAsync(
            streamPayload: payload,
            partitionKey: partitionKey).ConfigureAwait(false);
        if (result.IsSuccessStatusCode)
        {
            var newItem = Serializer.FromStream<T>(result.Content) ?? throw new DbUnexpectedStateException("FromStream<T> returned null");
            Debug.Assert(item.id == newItem.id);
            Debug.Assert(item.pk == newItem.pk);
            Debug.Assert(item.Type == type);
            Debug.Assert(item._etag != newItem._etag);
            return new CreateResult<T>(newItem);
        }
        return result.CreateResultFromErrorStatus<T>();
    }

    internal protected virtual async Task CreateOrReplaceItemWithoutResultAsync<T>(T item, string type, PartitionKey partitionKey, string partitionKeyString, bool allowTtl) where T : DbDoc
    {
        PrepareCreateItem(item, type, partitionKeyString);
        using var payload = Serializer.ToStream(item);
        using var result = await Container.UpsertItemStreamAsync(
            streamPayload: payload,
            partitionKey: partitionKey,
            requestOptions: OptionsWithoutContentResponse).ConfigureAwait(false);
        if (result.IsSuccessStatusCode)
        {
            return;
        }
        throw result.ExceptionFromErrorStatus();
    }
    internal protected virtual async Task<CreateOrReplaceResult<T>> CreateOrReplaceItemAsync<T>(T item, string type, PartitionKey partitionKey, string partitionKeyString, bool allowTtl) where T : DbDoc
    {
        PrepareCreateItem(item, type, partitionKeyString);
        using var payload = Serializer.ToStream(item);
        using var result = await Container.UpsertItemStreamAsync(
            streamPayload: payload,
            partitionKey: partitionKey).ConfigureAwait(false);
        if (result.IsSuccessStatusCode)
        {
            var newItem = Serializer.FromStream<T>(result.Content) ?? throw new DbUnexpectedStateException("FromStream<T> returned null");
            Debug.Assert(result.StatusCode == HttpStatusCode.OK || result.StatusCode == HttpStatusCode.Created);
            Debug.Assert(item.id == newItem.id);
            Debug.Assert(item.pk == newItem.pk);
            Debug.Assert(item.Type == type);
            Debug.Assert(item._etag != newItem._etag);
            return new CreateOrReplaceResult<T>(
                document: newItem,
                alreadyExisted: result.StatusCode != HttpStatusCode.Created);
        }
        throw result.ExceptionFromErrorStatus();
    }

    internal protected virtual async Task<ReadOrCreateResult<T>> ReadOrCreateItemAsync<T>(T item, string type, PartitionKey partitionKey, string partitionKeyString, bool tryCreateFirst) where T : DbDoc
    {
        PrepareCreateItem(item, type, partitionKeyString);
        const int ConcurrencyRetryCount = 10;
        for (var x = 0; x < ConcurrencyRetryCount; ++x)
        {
            if (tryCreateFirst)
            {
                using var payload = Serializer.ToStream(item);
                using var result = await Container.CreateItemStreamAsync(
                    streamPayload: payload,
                    partitionKey: partitionKey).ConfigureAwait(false);
                if (result.IsSuccessStatusCode)
                {
                    var newItem = Serializer.FromStream<T>(result.Content) ?? throw new DbUnexpectedStateException("FromStream<T> returned null");
                    Debug.Assert(result.StatusCode == HttpStatusCode.OK || result.StatusCode == HttpStatusCode.Created);
                    Debug.Assert(item.id == newItem.id);
                    Debug.Assert(item.pk == newItem.pk);
                    Debug.Assert(item.Type == type);
                    Debug.Assert(item._etag != newItem._etag);
                    return new ReadOrCreateResult<T>(
                        document: newItem,
                        alreadyExisted: false);
                }
                else if (result.StatusCode != HttpStatusCode.Conflict)
                {
                    throw result.ExceptionFromErrorStatus();
                }
            }

            tryCreateFirst = true;

            var readItem = await ReadByIdAsync<T>(
                id: item.id,
                partitionKey: partitionKey,
                type: type).ConfigureAwait(false);
            if (readItem is not null)
            {
                return new ReadOrCreateResult<T>(
                    document: readItem,
                    alreadyExisted: true);
            }
        }

        Debug.Assert(false);
        throw new DbOverloadedException(); // Should never get here
    }

    void PrepareReplaceItem<T>(T item, string type, string partitionKeyString, bool allowTtl) where T : DbDoc
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(type);
        ArgumentNullException.ThrowIfNull(partitionKeyString);

        ThrowIfReadOnly();

        if (item.id is null)
        {
            throw new InvalidOperationException("The document .id property is missing");
        }
        if (item._etag is null)
        {
            throw new InvalidOperationException("The document is missing an etag");
        }
        if (item.pk != partitionKeyString)
        {
            throw new InvalidOperationException("The document .pk property does not match this partition key");
        }
        if (item.Type != type)
        {
            throw new InvalidOperationException($"The document .Type property does not match what was expected ({type})");
        }
        if (item.ttl.HasValue && !allowTtl)
        {
            throw new InvalidOperationException($"The document .ttl property must be null");
        }
        if (item.ttl.HasValue && (item.ttl.Value < -1 || item.ttl.Value == 0))
        {
            throw new InvalidOperationException($"The document .ttl property must be either -1 or greater than 0, if provided");
        }

        if (ValidateStateBeforeSave)
        {
            item.ValidateStateOrThrow();
        }
    }
    internal protected virtual async Task<DbConflictType?> ReplaceItemWithoutResultAsync<T>(T item, string type, PartitionKey partitionKey, string partitionKeyString, bool allowTtl) where T : DbDoc
    {
        PrepareReplaceItem(item, type, partitionKeyString, allowTtl);

        using var payload = Serializer.ToStream(item);
        using var result = await Container.ReplaceItemStreamAsync(
            streamPayload: payload,
            id: item.id,
            partitionKey: partitionKey,
            requestOptions: new ItemRequestOptions { IfMatchEtag = item._etag, EnableContentResponseOnWrite = false }).ConfigureAwait(false);
        if (result.IsSuccessStatusCode)
        {
            return null;
        }
        return result.ReplaceConflictTypeFromErrorStatus();
    }
    internal protected virtual async Task<ReplaceResult<T>> ReplaceItemAsync<T>(T item, string type, PartitionKey partitionKey, string partitionKeyString, bool allowTtl) where T : DbDoc
    {
        PrepareReplaceItem(item, type, partitionKeyString, allowTtl);

        using var payload = Serializer.ToStream(item);
        using var result = await Container.ReplaceItemStreamAsync(
            streamPayload: payload,
            id: item.id,
            partitionKey: partitionKey,
            requestOptions: new ItemRequestOptions { IfMatchEtag = item._etag }).ConfigureAwait(false);
        if (result.IsSuccessStatusCode)
        {
            var newItem = Serializer.FromStream<T>(result.Content) ?? throw new DbUnexpectedStateException("FromStream<T> returned null");
            Debug.Assert(item.id == newItem.id);
            Debug.Assert(item.pk == newItem.pk);
            Debug.Assert(item.Type == type);
            Debug.Assert(item._etag != newItem._etag);
            return new ReplaceResult<T>(newItem);
        }
        return result.ReplaceResultFromErrorStatus<T>();
    }

    internal protected virtual async Task<DbConflictType?> DeleteItemAsync<T>(T item, PartitionKey partitionKey, string partitionKeyString) where T : DbDoc
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(partitionKeyString);

        ThrowIfReadOnly();

        if (item.id is null)
        {
            throw new InvalidOperationException("The document .id property is missing");
        }
        if (item._etag is null)
        {
            throw new InvalidOperationException("The document is missing an etag");
        }
        if (item.pk != partitionKeyString)
        {
            throw new InvalidOperationException("The document .pk property does not match this partition key");
        }

        using var result = await Container.DeleteItemStreamAsync(
            id: item.id,
            partitionKey: partitionKey,
            requestOptions: new ItemRequestOptions { IfMatchEtag = item._etag }).ConfigureAwait(false);
        if (result.IsSuccessStatusCode)
        {
            return null;
        }

        return result.DeleteConflictTypeFromErrorStatus();
    }

    /// <summary>
    /// Executes a query asynchronously.
    /// <see cref="https://github.com/Azure/azure-cosmos-dotnet-v3/blob/bb72ba5786d99d928b4774e16810f2655029e8a2/Microsoft.Azure.Cosmos/src/Linq/CosmosLinqExtensions.cs"/>
    /// </summary>
    public virtual async IAsyncEnumerable<T> ExecuteQueryAsync<T>(
        IQueryable<T> query,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);

        cancellationToken.ThrowIfCancellationRequested();
        var streamIterator = query.ToStreamIterator();
        while (streamIterator.HasMoreResults)
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var results = await streamIterator.ReadNextAsync(cancellationToken).ConfigureAwait(false);
            if (results.IsSuccessStatusCode)
            {
                foreach (var item in Serializer.DeserializeDocumentList<T>(results.Content))
                {
                    yield return item;
                }
            }
            else
            {
                throw results.ExceptionFromErrorStatus();
            }
        }
    }

#if DEBUG
    /// <summary>
    /// This deletes an item regardless of whether it's listed as "Transient"
    /// Use with care.
    /// This method only exists in DEBUG mode and is intended only for debugging purposes.
    /// </summary>
#pragma warning disable IDE1006 // Naming Styles
    public async Task<DbConflictType?> _DEBUGONLY_DANGEROUS_DIE_DIE_DIE_DeleteItemAsync<T>(T item) where T : DbDoc
#pragma warning restore IDE1006 // Naming Styles
    {
        ArgumentNullException.ThrowIfNull(item);

        ThrowIfReadOnly();

        if (item.id is null)
        {
            throw new InvalidOperationException("The document .id property is missing");
        }
        if (item._etag is null)
        {
            throw new InvalidOperationException("The document is missing an etag");
        }

        using var result = await Container.DeleteItemStreamAsync(
            id: item.id,
            partitionKey: new PartitionKey(item.pk),
            requestOptions: new ItemRequestOptions { IfMatchEtag = item._etag }).ConfigureAwait(false);
        if (result.IsSuccessStatusCode)
        {
            return null;
        }

        return result.DeleteConflictTypeFromErrorStatus();
    }
#endif
}

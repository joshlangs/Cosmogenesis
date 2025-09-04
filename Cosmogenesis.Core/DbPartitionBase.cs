using Microsoft.Azure.Cosmos;

namespace Cosmogenesis.Core;

public abstract class DbPartitionBase
{
    protected virtual string PartitionKeyString { get; } = default!;
    protected virtual DbBase DB { get; } = default!;
    protected virtual DbSerializerBase Serializer { get; } = default!;
    protected virtual PartitionKey PartitionKey { get; } = default!;

    protected DbPartitionBase() { }

    protected DbPartitionBase(DbBase db, string partitionKey, DbSerializerBase serializer)
    {
        if (string.IsNullOrEmpty(partitionKey))
        {
            throw new ArgumentNullException(nameof(partitionKey));
        }

        DB = db ?? throw new ArgumentNullException(nameof(db));
        PartitionKeyString = partitionKey;
        Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        PartitionKey = new PartitionKey(partitionKey);
    }

    protected TransactionalBatch CreateBatchForPartition()
    {
        DB.ThrowIfReadOnly();
        return DB.Container.CreateTransactionalBatch(PartitionKey);
    }

    protected Task<CreateResult<T>> CreateItemAsync<T>(T item, string type) where T : DbDoc =>
        DB.CreateItemAsync(
            item: item,
            type: type,
            partitionKey: PartitionKey,
            partitionKeyString: PartitionKeyString);

    protected Task<ReadOrCreateResult<T>> ReadOrCreateItemAsync<T>(T item, string type, bool tryCreateFirst) where T : DbDoc =>
        DB.ReadOrCreateItemAsync(
            item: item,
            type: type,
            partitionKey: PartitionKey,
            partitionKeyString: PartitionKeyString,
            tryCreateFirst: tryCreateFirst);

    protected Task<CreateOrReplaceResult<T>> CreateOrReplaceItemAsync<T>(T item, string type, bool allowTtl) where T : DbDoc =>
        DB.CreateOrReplaceItemAsync(
            item: item,
            type: type,
            partitionKey: PartitionKey,
            partitionKeyString: PartitionKeyString,
            allowTtl: allowTtl);

    protected Task<ReplaceResult<T>> ReplaceItemAsync<T>(T item, string type, bool allowTtl) where T : DbDoc =>
        DB.ReplaceItemAsync(
            item: item,
            type: type,
            partitionKey: PartitionKey,
            partitionKeyString: PartitionKeyString,
            allowTtl: allowTtl);

    protected Task<DbConflictType?> DeleteItemAsync<T>(T item) where T : DbDoc =>
        DB.DeleteItemAsync(
            item: item,
            partitionKey: PartitionKey,
            partitionKeyString: PartitionKeyString);

}

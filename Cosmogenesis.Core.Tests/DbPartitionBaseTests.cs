using Microsoft.Azure.Cosmos;
using Moq;

namespace Cosmogenesis.Core.Tests;

public class DbPartitionBaseTests
{
    class TestPartition(DbBase db, string partitionKey, DbSerializerBase serializer) : DbPartitionBase(db, partitionKey, serializer)
    {
        public new TransactionalBatch CreateBatchForPartition() => base.CreateBatchForPartition();
        public new Task<CreateResult<T>> CreateItemAsync<T>(T item, string type) where T : DbDoc => base.CreateItemAsync(item, type);
        public new Task<ReadOrCreateResult<T>> ReadOrCreateItemAsync<T>(T item, string type, bool tryCreateFirst) where T : DbDoc => base.ReadOrCreateItemAsync(item, type, tryCreateFirst);
        public new Task<CreateOrReplaceResult<T>> CreateOrReplaceItemAsync<T>(T item, string type, bool allowTtl) where T : DbDoc => base.CreateOrReplaceItemAsync(item, type, allowTtl);
        public new Task<ReplaceResult<T>> ReplaceItemAsync<T>(T item, string type, bool allowTtl) where T : DbDoc => base.ReplaceItemAsync(item, type, allowTtl);
        public new Task<DbConflictType?> DeleteItemAsync<T>(T item) where T : DbDoc => base.DeleteItemAsync(item);
    }

    readonly Mock<DbBase> MockDb = new(MockBehavior.Strict);
    readonly Mock<DbSerializerBase> MockSerializer = new(MockBehavior.Strict);
    readonly Mock<TransactionalBatch> MockBatch = new(MockBehavior.Strict);
    const string PartitionKeyString = "testpk";
    readonly PartitionKey PartitionKey = new(PartitionKeyString);
    const string Type = "testtype";


    [Fact]
    [Trait("Type", "Unit")]
    public void Ctor_NullDb_Throws() => Assert.Throws<ArgumentNullException>(() => new TestPartition(null!, "a", MockSerializer.Object));

    [Fact]
    [Trait("Type", "Unit")]
    public void Ctor_NullPartitionKey_Throws() => Assert.Throws<ArgumentNullException>(() => new TestPartition(MockDb.Object, null!, MockSerializer.Object));

    [Fact]
    [Trait("Type", "Unit")]
    public void Ctor_EmptyPartitionKey_Throws() => Assert.Throws<ArgumentException>(() => new TestPartition(MockDb.Object, "", MockSerializer.Object));

    [Fact]
    [Trait("Type", "Unit")]
    public void Ctor_NullSerializer_Throws() => Assert.Throws<ArgumentNullException>(() => new TestPartition(MockDb.Object, "a", null!));

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateBatchForPartition_NotReadOnly_CreatesBatch()
    {
        MockDb.Setup(x => x.IsReadOnly).Returns(false).Verifiable();
        MockDb.Setup(x => x.Container.CreateTransactionalBatch(PartitionKey)).Returns(MockBatch.Object).Verifiable();
        var partition = new DbPartitionBaseTests.TestPartition(MockDb.Object, PartitionKeyString, MockSerializer.Object);

        var batch = partition.CreateBatchForPartition();

        MockDb.Verify();
        Assert.Equal(MockBatch.Object, batch);
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateBatchForPartition_ReadOnly_Throws()
    {
        MockDb.Setup(x => x.IsReadOnly).Returns(true).Verifiable();
        var partition = new DbPartitionBaseTests.TestPartition(MockDb.Object, PartitionKeyString, MockSerializer.Object);

        Assert.Throws<InvalidOperationException>(() => partition.CreateBatchForPartition());
        MockDb.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public async Task CreateItemAsync_CallsDB()
    {
        var actualResult = DbModelFactory.CreateCreateResult(TestDoc.Instance);
        MockDb.Setup(x => x.CreateItemAsync(TestDoc.Instance, Type, PartitionKey, PartitionKeyString)).Returns(Task.FromResult(actualResult)).Verifiable();
        var partition = new DbPartitionBaseTests.TestPartition(MockDb.Object, PartitionKeyString, MockSerializer.Object);

        var result = await partition.CreateItemAsync(TestDoc.Instance, Type);

        MockDb.Verify();
        Assert.Same(result, actualResult);
    }

    [Fact]
    [Trait("Type", "Unit")]
    public async Task ReadOrCreateItemAsync_CallsDB()
    {
        var actualResult = DbModelFactory.CreateReadOrCreateResult(TestDoc.Instance, true);
        MockDb.Setup(x => x.ReadOrCreateItemAsync(TestDoc.Instance, Type, PartitionKey, PartitionKeyString, true)).Returns(Task.FromResult(actualResult)).Verifiable();
        var partition = new DbPartitionBaseTests.TestPartition(MockDb.Object, PartitionKeyString, MockSerializer.Object);

        var result = await partition.ReadOrCreateItemAsync(TestDoc.Instance, Type, true);

        MockDb.Verify();
        Assert.Same(result, actualResult);
    }

    [Fact]
    [Trait("Type", "Unit")]
    public async Task CreateOrReplaceItemAsync_CallsDB()
    {
        var actualResult = DbModelFactory.CreateCreateOrReplaceResult(TestDoc.Instance, true);
        MockDb.Setup(x => x.CreateOrReplaceItemAsync(TestDoc.Instance, Type, PartitionKey, PartitionKeyString, true)).Returns(Task.FromResult(actualResult)).Verifiable();
        var partition = new DbPartitionBaseTests.TestPartition(MockDb.Object, PartitionKeyString, MockSerializer.Object);

        var result = await partition.CreateOrReplaceItemAsync(TestDoc.Instance, Type, true);

        MockDb.Verify();
        Assert.Same(result, actualResult);
    }

    [Fact]
    [Trait("Type", "Unit")]
    public async Task ReplaceItemAsync_CallsDB()
    {
        var actualResult = DbModelFactory.CreateReplaceResult(TestDoc.Instance);
        MockDb.Setup(x => x.ReplaceItemAsync(TestDoc.Instance, Type, PartitionKey, PartitionKeyString, true)).Returns(Task.FromResult(actualResult)).Verifiable();
        var partition = new DbPartitionBaseTests.TestPartition(MockDb.Object, PartitionKeyString, MockSerializer.Object);

        var result = await partition.ReplaceItemAsync(TestDoc.Instance, Type, true);

        MockDb.Verify();
        Assert.Same(result, actualResult);
    }

    [Fact]
    [Trait("Type", "Unit")]
    public async Task DeleteItemAsync_CallsDB()
    {
        var actualResult = (DbConflictType?)DbConflictType.ETagChanged;
        MockDb.Setup(x => x.DeleteItemAsync(TestDoc.Instance, PartitionKey, PartitionKeyString)).Returns(Task.FromResult(actualResult)).Verifiable();
        var partition = new DbPartitionBaseTests.TestPartition(MockDb.Object, PartitionKeyString, MockSerializer.Object);

        var result = await partition.DeleteItemAsync(TestDoc.Instance);

        MockDb.Verify();
        Assert.Equal(result, actualResult);
    }
}

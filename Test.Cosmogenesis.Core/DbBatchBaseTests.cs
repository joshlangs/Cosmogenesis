using Microsoft.Azure.Cosmos;
using Moq;

namespace Test.Cosmogenesis.Core;

public class DbBatchBaseTests
{
    class TestBatch(DbSerializerBase serializer, TransactionalBatch transactionalBatch, string partitionKey) : DbBatchBase(serializer, transactionalBatch, partitionKey, true, true)
    {
        public new void CreateCore<T>(T item, string type) where T : DbDoc => base.CreateCore(item, type);
        public new void DeleteCore<T>(T item) where T : DbDoc => base.DeleteCore(item);
        public new void ReplaceCore<T>(T item, string type) where T : DbDoc => base.ReplaceCore(item, type);
        public new void CreateOrReplaceCore<T>(T item, string type) where T : DbDoc => base.CreateOrReplaceCore(item, type);
        public new Task<bool> ExecuteCoreAsync() => base.ExecuteCoreAsync();
        public new Task ExecuteOrThrowCoreAsync() => base.ExecuteOrThrowCoreAsync();
        public new Task<BatchResult> ExecuteWithResultsCoreAsync() => base.ExecuteWithResultsCoreAsync();
    }

    readonly Mock<TransactionalBatch> MockBatch = new(MockBehavior.Strict);
    readonly Mock<DbSerializerBase> MockSerializer = new(MockBehavior.Strict);
    const string PartitionKeyString = "asdf";

    readonly TestDoc TestDocWithETag = new()
    {
        id = "asdf",
        pk = PartitionKeyString,
        Type = "asdf",
        _etag = "asdf"
    };
    readonly TestDoc TestDocWithoutETag = new()
    {
        id = "asdf",
        pk = PartitionKeyString,
        Type = "asdf"
    };

    TestBatch CreateTestBatch() => new(MockSerializer.Object, MockBatch.Object, PartitionKeyString);

    [Test]
    public async Task Ctor_NullSerializer_Throws() => Assert.Throws<ArgumentNullException>(() => new TestBatch(null!, MockBatch.Object, PartitionKeyString));

    [Test]
    public async Task Ctor_NullBatch_Throws() => Assert.Throws<ArgumentNullException>(() => new TestBatch(MockSerializer.Object, null!, PartitionKeyString));

    [Test]
    public async Task Ctor_NullPartitionKey_Throws() => Assert.Throws<ArgumentNullException>(() => new TestBatch(MockSerializer.Object, MockBatch.Object, null!));

    [Test]
    public async Task IsEmpty_StartsTrue() => await Assert.That(CreateTestBatch().IsEmpty).IsTrue();

    [Test]
    public async Task ExecuteCoreAsync_Empty_ReturnsCompletedSynchronously()
    {
        var batch = CreateTestBatch();

        var result = batch.ExecuteCoreAsync();

        await Assert.That(result.IsCompletedSuccessfully).IsTrue();
        await Assert.That(result.Result).IsTrue();
    }

    [Test]
    public async Task ExecuteCoreAsync_Twice_Throws()
    {
        var batch = CreateTestBatch();

        await batch.ExecuteCoreAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => batch.ExecuteCoreAsync());
    }

    [Test]
    public async Task ExecuteOrThrowCoreAsync_Empty_ReturnsCompletedSynchronously()
    {
        var batch = CreateTestBatch();

        var result = batch.ExecuteOrThrowCoreAsync();

        await Assert.That(result.IsCompletedSuccessfully).IsTrue();
    }

    [Test]
    public async Task ExecuteOrThrowCoreAsync_Twice_Throws()
    {
        var batch = CreateTestBatch();

        await batch.ExecuteOrThrowCoreAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => batch.ExecuteOrThrowCoreAsync());
    }

    [Test]
    public async Task ExecuteWithResultsCoreAsync_Empty_ReturnsCompletedSynchronously()
    {
        var batch = CreateTestBatch();

        var result = batch.ExecuteWithResultsCoreAsync();

        await Assert.That(result.IsCompletedSuccessfully).IsTrue();
        Assert.Null(result.Result.Conflict);
        await Assert.That(result.Result.Documents!).IsEmpty();
    }

    [Test]
    public async Task ExecuteWithResultsCoreAsync_Twice_Throws()
    {
        var batch = CreateTestBatch();

        await batch.ExecuteWithResultsCoreAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => batch.ExecuteWithResultsCoreAsync());
    }

    [Test]
    public async Task DeleteCore_Null_Throws() => Assert.Throws<ArgumentNullException>(() => CreateTestBatch().DeleteCore<DbDoc>(null!));

    [Test]
    public async Task DeleteCore_NullId_Throws()
    {
        TestDocWithETag.id = null!;
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().DeleteCore(TestDocWithETag));
    }

    [Test]
    public async Task DeleteCore_NullETag_Throws() => Assert.Throws<InvalidOperationException>(() => CreateTestBatch().DeleteCore(TestDocWithoutETag));

    [Test]
    public async Task DeleteCore_WrongPK_Throws()
    {
        TestDocWithETag.pk += "a";
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().DeleteCore(TestDocWithETag));
    }

    [Test]
    public async Task DeleteCore_SetsIsEmptyFalse()
    {
        var fail = false;
        MockBatch
            .Setup(x => x.DeleteItem(TestDocWithETag.id, It.IsAny<TransactionalBatchItemRequestOptions>()))
            .Returns((string id, TransactionalBatchItemRequestOptions o) =>
            {
                if (TestDocWithETag.id != id) { fail = true; }
                if (o.IfMatchEtag != TestDocWithETag._etag) { fail = true; }
                return MockBatch.Object;
            })
            .Verifiable();

        await Assert.That(fail).IsFalse();

        var batch = CreateTestBatch();
        batch.DeleteCore(TestDocWithETag);
        await Assert.That(batch.IsEmpty).IsFalse();
        MockBatch.Verify();
    }

    [Test]
    public async Task DeleteCore_DuplicateETag_Throws()
    {
        var fail = false;
        MockBatch
            .Setup(x => x.DeleteItem(TestDocWithETag.id, It.IsAny<TransactionalBatchItemRequestOptions>()))
            .Returns((string id, TransactionalBatchItemRequestOptions o) =>
            {
                if (TestDocWithETag.id != id) { fail = true; }
                if (o.IfMatchEtag != TestDocWithETag._etag) { fail = true; }
                return MockBatch.Object;
            })
            .Verifiable();

        await Assert.That(fail).IsFalse();

        var batch = CreateTestBatch();
        batch.DeleteCore(TestDocWithETag);
        Assert.Throws<InvalidOperationException>(() => batch.DeleteCore(TestDocWithETag));
        MockBatch.Verify();
    }

    [Test]
    public async Task DeleteCore_AfterExecute_Throws()
    {
        var batch = CreateTestBatch();
        await batch.ExecuteCoreAsync();
        Assert.Throws<InvalidOperationException>(() => batch.DeleteCore(TestDocWithETag));
    }


    [Test]
    public async Task ReplaceCore_NullItem_Throws() => Assert.Throws<ArgumentNullException>(() => CreateTestBatch().ReplaceCore<DbDoc>(null!, "asdf"));

    [Test]
    public async Task ReplaceCore_NullType_Throws() => Assert.Throws<ArgumentNullException>(() => CreateTestBatch().ReplaceCore(TestDocWithETag, null!));

    [Test]
    public async Task ReplaceCore_NullId_Throws()
    {
        TestDocWithETag.id = null!;
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().ReplaceCore(TestDocWithETag, TestDocWithETag.Type));
    }

    [Test]
    public async Task ReplaceCore_NullETag_Throws() => Assert.Throws<InvalidOperationException>(() => CreateTestBatch().ReplaceCore(TestDocWithoutETag, TestDocWithETag.Type));

    [Test]
    public async Task ReplaceCore_WrongPK_Throws()
    {
        TestDocWithETag.pk += "a";
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().ReplaceCore(TestDocWithETag, TestDocWithETag.Type));
    }

    [Test]
    public async Task ReplaceCore_WrongType_Throws() => Assert.Throws<InvalidOperationException>(() => CreateTestBatch().ReplaceCore(TestDocWithETag, TestDocWithETag.Type + "a"));

    [Test]
    public async Task ReplaceCore_SetsIsEmptyFalse()
    {
        var ms = new MemoryStream();
        var fail = false;
        MockSerializer.Setup(x => x.ToStream(TestDocWithETag)).Returns(ms).Verifiable();
        MockBatch
            .Setup(x => x.ReplaceItemStream(TestDocWithETag.id, ms, It.IsAny<TransactionalBatchItemRequestOptions>()))
            .Returns((string id, Stream s, TransactionalBatchItemRequestOptions o) =>
            {
                if (TestDocWithETag.id != id) { fail = true; }
                if (!ReferenceEquals(ms, s)) { fail = true; }
                if (o.IfMatchEtag != TestDocWithETag._etag) { fail = true; }
                return MockBatch.Object;
            })
            .Verifiable();

        await Assert.That(fail).IsFalse();

        var batch = CreateTestBatch();
        batch.ReplaceCore(TestDocWithETag, TestDocWithETag.Type);
        await Assert.That(batch.IsEmpty).IsFalse();
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Test]
    public async Task ReplaceCore_DuplicateETag_Throws()
    {
        var ms = new MemoryStream();
        var fail = false;
        MockSerializer.Setup(x => x.ToStream(TestDocWithETag)).Returns(ms).Verifiable();
        MockBatch
            .Setup(x => x.ReplaceItemStream(TestDocWithETag.id, ms, It.IsAny<TransactionalBatchItemRequestOptions>()))
            .Returns((string id, Stream s, TransactionalBatchItemRequestOptions o) =>
            {
                if (TestDocWithETag.id != id) { fail = true; }
                if (!ReferenceEquals(ms, s)) { fail = true; }
                if (o.IfMatchEtag != TestDocWithETag._etag) { fail = true; }
                return MockBatch.Object;
            })
            .Verifiable();

        await Assert.That(fail).IsFalse();

        var batch = CreateTestBatch();
        batch.ReplaceCore(TestDocWithETag, TestDocWithETag.Type);
        Assert.Throws<InvalidOperationException>(() => batch.ReplaceCore(TestDocWithETag, TestDocWithETag.Type));
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Test]
    public async Task ReplaceCore_AfterExecute_Throws()
    {
        var batch = CreateTestBatch();
        await batch.ExecuteCoreAsync();
        Assert.Throws<InvalidOperationException>(() => batch.ReplaceCore(TestDocWithETag, TestDocWithETag.Type));
    }



    [Test]
    public async Task CreateCore_NullItem_Throws() => Assert.Throws<ArgumentNullException>(() => CreateTestBatch().CreateCore<DbDoc>(null!, "asdf"));

    [Test]
    public async Task CreateCore_NullType_Throws() => Assert.Throws<ArgumentNullException>(() => CreateTestBatch().CreateCore(TestDocWithoutETag, null!));

    [Test]
    public async Task CreateCore_NullId_Throws()
    {
        TestDocWithoutETag.id = null!;
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type));
    }

    [Test]
    public async Task CreateCore_NonNullETag_Throws() => Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateCore(TestDocWithETag, TestDocWithETag.Type));

    [Test]
    public async Task CreateCore_WrongPK_Throws()
    {
        TestDocWithoutETag.pk += "a";
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type));
    }

    [Test]
    public async Task CreateCore_WrongType_Throws() => Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type + "a"));

    [Test]
    public async Task CreateCore_SetsIsEmptyFalse()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.CreateItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type);
        await Assert.That(batch.IsEmpty).IsFalse();
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Test]
    public async Task CreateCore_AfterExecute_Throws()
    {
        var batch = CreateTestBatch();
        await batch.ExecuteCoreAsync();
        Assert.Throws<InvalidOperationException>(() => batch.CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type));
    }

    [Test]
    public async Task CreateCore_NullType_SetsType()
    {
        var ms = new MemoryStream();
        var type = TestDocWithoutETag.Type;
        TestDocWithoutETag.Type = null!;
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.CreateItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateCore(TestDocWithoutETag, type);
        await Assert.That(TestDocWithoutETag.Type).IsEqualTo(type);
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Test]
    public async Task CreateCore_NullPk_SetsPk()
    {
        var ms = new MemoryStream();
        TestDocWithoutETag.pk = null!;
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.CreateItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type);
        await Assert.That(TestDocWithoutETag.pk).IsEqualTo(PartitionKeyString);
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Test]
    public async Task CreateCore_SetsCreationDate()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.CreateItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type);
        await Assert.That(Math.Abs(TestDocWithoutETag.CreationDate.Subtract(DateTime.UtcNow).TotalSeconds) < 5).IsTrue();
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Test]
    public async Task CreateOrReplaceCore_NullItem_Throws() => Assert.Throws<ArgumentNullException>(() => CreateTestBatch().CreateOrReplaceCore<DbDoc>(null!, "asdf"));

    [Test]
    public async Task CreateOrReplaceCore_NullType_Throws() => Assert.Throws<ArgumentNullException>(() => CreateTestBatch().CreateOrReplaceCore(TestDocWithoutETag, null!));

    [Test]
    public async Task CreateOrReplaceCore_NullId_Throws()
    {
        TestDocWithoutETag.id = null!;
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateOrReplaceCore(TestDocWithoutETag, TestDocWithoutETag.Type));
    }

    [Test]
    public async Task CreateOrReplaceCore_NonNullETag_Throws() => Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateOrReplaceCore(TestDocWithETag, TestDocWithETag.Type));

    [Test]
    public async Task CreateOrReplaceCore_WrongPK_Throws()
    {
        TestDocWithoutETag.pk += "a";
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateOrReplaceCore(TestDocWithoutETag, TestDocWithoutETag.Type));
    }

    [Test]
    public async Task CreateOrReplaceCore_WrongType_Throws() => Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateOrReplaceCore(TestDocWithoutETag, TestDocWithoutETag.Type + "a"));

    [Test]
    public async Task CreateOrReplaceCore_SetsIsEmptyFalse()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.UpsertItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateOrReplaceCore(TestDocWithoutETag, TestDocWithoutETag.Type);
        await Assert.That(batch.IsEmpty).IsFalse();
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Test]
    public async Task CreateOrReplaceCore_AfterExecute_Throws()
    {
        var batch = CreateTestBatch();
        await batch.ExecuteCoreAsync();
        Assert.Throws<InvalidOperationException>(() => batch.CreateOrReplaceCore(TestDocWithoutETag, TestDocWithoutETag.Type));
    }

    [Test]
    public async Task CreateOrReplaceCore_NullType_SetsType()
    {
        var ms = new MemoryStream();
        var type = TestDocWithoutETag.Type;
        TestDocWithoutETag.Type = null!;
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.UpsertItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateOrReplaceCore(TestDocWithoutETag, type);
        await Assert.That(TestDocWithoutETag.Type).IsEqualTo(type);
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Test]
    public async Task CreateOrReplaceCore_NullPk_SetsPk()
    {
        var ms = new MemoryStream();
        TestDocWithoutETag.pk = null!;
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.UpsertItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateOrReplaceCore(TestDocWithoutETag, TestDocWithoutETag.Type);
        await Assert.That(TestDocWithoutETag.pk).IsEqualTo(PartitionKeyString);
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Test]
    public async Task CreateOrReplaceCore_SetsCreationDate()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.UpsertItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateOrReplaceCore(TestDocWithoutETag, TestDocWithoutETag.Type);
        await Assert.That(Math.Abs(TestDocWithoutETag.CreationDate.Subtract(DateTime.UtcNow).TotalSeconds) < 5).IsTrue();
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Test]
    public async Task ExecuteCoreAsync_BatchSuccess_ReturnsTrue()
    {
        var mockResponse = new Mock<TransactionalBatchResponse>();
        mockResponse.Setup(x => x.IsSuccessStatusCode).Returns(true).Verifiable();
        mockResponse.Setup(x => x.Count).Returns(1).Verifiable();
        var mockOpResponse = new Mock<TransactionalBatchOperationResult>(MockBehavior.Strict);
        mockOpResponse.Setup(x => x.IsSuccessStatusCode).Returns(true).Verifiable();
        mockResponse.Setup(x => x[0]).Returns(mockOpResponse.Object).Verifiable();
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.CreateItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();
        MockBatch.Setup(x => x.ExecuteAsync(It.IsAny<CancellationToken>())).Returns(Task.FromResult(mockResponse.Object)).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type);
        var result = await batch.ExecuteCoreAsync();

        await Assert.That(result).IsTrue();
        mockResponse.Verify();
        mockOpResponse.Verify();
        MockSerializer.Verify();
        MockBatch.Verify();
    }
}

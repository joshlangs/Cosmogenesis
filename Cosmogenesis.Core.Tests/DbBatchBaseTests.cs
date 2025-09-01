using Microsoft.Azure.Cosmos;
using Moq;

namespace Cosmogenesis.Core.Tests;

public class DbBatchBaseTests
{
    class TestBatch : DbBatchBase
    {
        public TestBatch(DbSerializerBase serializer, TransactionalBatch transactionalBatch, string partitionKey) : base(serializer, transactionalBatch, partitionKey, true)
        {
        }
        public new void CreateCore<T>(T item, string type) where T : DbDoc => base.CreateCore(item, type);
        public new void DeleteCore<T>(T item) where T : DbDoc => base.DeleteCore(item);
        public new void ReplaceCore<T>(T item, string type) where T : DbDoc => base.ReplaceCore(item, type);
        public new void CreateOrReplaceCore<T>(T item, string type) where T : DbDoc => base.CreateOrReplaceCore(item, type);
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

    [Fact]
    [Trait("Type", "Unit")]
    public void Ctor_NullSerializer_Throws() => Assert.Throws<ArgumentNullException>(() => new TestBatch(null!, MockBatch.Object, PartitionKeyString));

    [Fact]
    [Trait("Type", "Unit")]
    public void Ctor_NullBatch_Throws() => Assert.Throws<ArgumentNullException>(() => new TestBatch(MockSerializer.Object, null!, PartitionKeyString));

    [Fact]
    [Trait("Type", "Unit")]
    public void Ctor_NullPartitionKey_Throws() => Assert.Throws<ArgumentNullException>(() => new TestBatch(MockSerializer.Object, MockBatch.Object, null!));

    [Fact]
    [Trait("Type", "Unit")]
    public void IsEmpty_StartsTrue() => Assert.True(CreateTestBatch().IsEmpty);

    [Fact]
    [Trait("Type", "Unit")]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "xUnit1031:Do not use blocking task operations in test method", Justification = "<Pending>")]
    public void ExecuteAsync_Empty_ReturnsCompletedSynchronously()
    {
        var batch = CreateTestBatch();

        var result = batch.ExecuteAsync();

        Assert.True(result.IsCompletedSuccessfully);
        Assert.True(result.Result);
    }

    [Fact]
    [Trait("Type", "Unit")]
    public async Task ExecuteAsync_Twice_Throws()
    {
        var batch = CreateTestBatch();

        await batch.ExecuteAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => batch.ExecuteAsync());
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void ExecuteOrThrowAsync_Empty_ReturnsCompletedSynchronously()
    {
        var batch = CreateTestBatch();

        var result = batch.ExecuteOrThrowAsync();

        Assert.True(result.IsCompletedSuccessfully);
    }

    [Fact]
    [Trait("Type", "Unit")]
    public async Task ExecuteOrThrowAsync_Twice_Throws()
    {
        var batch = CreateTestBatch();

        await batch.ExecuteOrThrowAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => batch.ExecuteOrThrowAsync());
    }

    [Fact]
    [Trait("Type", "Unit")]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "xUnit1031:Do not use blocking task operations in test method", Justification = "<Pending>")]
    public void ExecuteWithResultsAsync_Empty_ReturnsCompletedSynchronously()
    {
        var batch = CreateTestBatch();

        var result = batch.ExecuteWithResultsAsync();

        Assert.True(result.IsCompletedSuccessfully);
        Assert.Null(result.Result.Conflict);
        Assert.Empty(result.Result.Documents!);
    }

    [Fact]
    [Trait("Type", "Unit")]
    public async Task ExecuteWithResultsAsync_Twice_Throws()
    {
        var batch = CreateTestBatch();

        await batch.ExecuteWithResultsAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => batch.ExecuteWithResultsAsync());
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void DeleteCore_Null_Throws() => Assert.Throws<ArgumentNullException>(() => CreateTestBatch().DeleteCore<DbDoc>(null!));

    [Fact]
    [Trait("Type", "Unit")]
    public void DeleteCore_NullId_Throws()
    {
        TestDocWithETag.id = null!;
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().DeleteCore(TestDocWithETag));
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void DeleteCore_NullETag_Throws() => Assert.Throws<InvalidOperationException>(() => CreateTestBatch().DeleteCore(TestDocWithoutETag));

    [Fact]
    [Trait("Type", "Unit")]
    public void DeleteCore_WrongPK_Throws()
    {
        TestDocWithETag.pk += "a";
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().DeleteCore(TestDocWithETag));
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void DeleteCore_SetsIsEmptyFalse()
    {
        MockBatch
            .Setup(x => x.DeleteItem(TestDocWithETag.id, It.IsAny<TransactionalBatchItemRequestOptions>()))
            .Returns((string id, TransactionalBatchItemRequestOptions o) =>
            {
                Assert.Equal(id, TestDocWithETag.id);
                Assert.Equal(o.IfMatchEtag, TestDocWithETag._etag);
                return MockBatch.Object;
            })
            .Verifiable();

        var batch = CreateTestBatch();
        batch.DeleteCore(TestDocWithETag);
        Assert.False(batch.IsEmpty);
        MockBatch.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void DeleteCore_DuplicateETag_Throws()
    {
        MockBatch
            .Setup(x => x.DeleteItem(TestDocWithETag.id, It.IsAny<TransactionalBatchItemRequestOptions>()))
            .Returns((string id, TransactionalBatchItemRequestOptions o) =>
            {
                Assert.Equal(id, TestDocWithETag.id);
                Assert.Equal(o.IfMatchEtag, TestDocWithETag._etag);
                return MockBatch.Object;
            })
            .Verifiable();

        var batch = CreateTestBatch();
        batch.DeleteCore(TestDocWithETag);
        Assert.Throws<InvalidOperationException>(() => batch.DeleteCore(TestDocWithETag));
        MockBatch.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public async Task DeleteCore_AfterExecute_Throws()
    {
        var batch = CreateTestBatch();
        await batch.ExecuteAsync();
        Assert.Throws<InvalidOperationException>(() => batch.DeleteCore(TestDocWithETag));
    }


    [Fact]
    [Trait("Type", "Unit")]
    public void ReplaceCore_NullItem_Throws() => Assert.Throws<ArgumentNullException>(() => CreateTestBatch().ReplaceCore<DbDoc>(null!, "asdf"));

    [Fact]
    [Trait("Type", "Unit")]
    public void ReplaceCore_NullType_Throws() => Assert.Throws<ArgumentNullException>(() => CreateTestBatch().ReplaceCore(TestDocWithETag, null!));

    [Fact]
    [Trait("Type", "Unit")]
    public void ReplaceCore_NullId_Throws()
    {
        TestDocWithETag.id = null!;
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().ReplaceCore(TestDocWithETag, TestDocWithETag.Type));
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void ReplaceCore_NullETag_Throws() => Assert.Throws<InvalidOperationException>(() => CreateTestBatch().ReplaceCore(TestDocWithoutETag, TestDocWithETag.Type));

    [Fact]
    [Trait("Type", "Unit")]
    public void ReplaceCore_WrongPK_Throws()
    {
        TestDocWithETag.pk += "a";
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().ReplaceCore(TestDocWithETag, TestDocWithETag.Type));
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void ReplaceCore_WrongType_Throws() => Assert.Throws<InvalidOperationException>(() => CreateTestBatch().ReplaceCore(TestDocWithETag, TestDocWithETag.Type + "a"));

    [Fact]
    [Trait("Type", "Unit")]
    public void ReplaceCore_SetsIsEmptyFalse()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithETag)).Returns(ms).Verifiable();
        MockBatch
            .Setup(x => x.ReplaceItemStream(TestDocWithETag.id, ms, It.IsAny<TransactionalBatchItemRequestOptions>()))
            .Returns((string id, Stream s, TransactionalBatchItemRequestOptions o) =>
            {
                Assert.Equal(id, TestDocWithETag.id);
                Assert.Same(s, ms);
                Assert.Equal(o.IfMatchEtag, TestDocWithETag._etag);
                return MockBatch.Object;
            })
            .Verifiable();

        var batch = CreateTestBatch();
        batch.ReplaceCore(TestDocWithETag, TestDocWithETag.Type);
        Assert.False(batch.IsEmpty);
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void ReplaceCore_DuplicateETag_Throws()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithETag)).Returns(ms).Verifiable();
        MockBatch
            .Setup(x => x.ReplaceItemStream(TestDocWithETag.id, ms, It.IsAny<TransactionalBatchItemRequestOptions>()))
            .Returns((string id, Stream s, TransactionalBatchItemRequestOptions o) =>
            {
                Assert.Equal(id, TestDocWithETag.id);
                Assert.Same(s, ms);
                Assert.Equal(o.IfMatchEtag, TestDocWithETag._etag);
                return MockBatch.Object;
            })
            .Verifiable();

        var batch = CreateTestBatch();
        batch.ReplaceCore(TestDocWithETag, TestDocWithETag.Type);
        Assert.Throws<InvalidOperationException>(() => batch.ReplaceCore(TestDocWithETag, TestDocWithETag.Type));
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public async Task ReplaceCore_AfterExecute_Throws()
    {
        var batch = CreateTestBatch();
        await batch.ExecuteAsync();
        Assert.Throws<InvalidOperationException>(() => batch.ReplaceCore(TestDocWithETag, TestDocWithETag.Type));
    }



    [Fact]
    [Trait("Type", "Unit")]
    public void CreateCore_NullItem_Throws() => Assert.Throws<ArgumentNullException>(() => CreateTestBatch().CreateCore<DbDoc>(null!, "asdf"));

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateCore_NullType_Throws() => Assert.Throws<ArgumentNullException>(() => CreateTestBatch().CreateCore(TestDocWithoutETag, null!));

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateCore_NullId_Throws()
    {
        TestDocWithoutETag.id = null!;
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type));
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateCore_NonNullETag_Throws() => Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateCore(TestDocWithETag, TestDocWithETag.Type));

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateCore_WrongPK_Throws()
    {
        TestDocWithoutETag.pk += "a";
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type));
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateCore_WrongType_Throws() => Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type + "a"));

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateCore_SetsIsEmptyFalse()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.CreateItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type);
        Assert.False(batch.IsEmpty);
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public async Task CreateCore_AfterExecute_Throws()
    {
        var batch = CreateTestBatch();
        await batch.ExecuteAsync();
        Assert.Throws<InvalidOperationException>(() => batch.CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type));
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateCore_NullType_SetsType()
    {
        var ms = new MemoryStream();
        var type = TestDocWithoutETag.Type;
        TestDocWithoutETag.Type = null!;
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.CreateItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateCore(TestDocWithoutETag, type);
        Assert.Equal(type, TestDocWithoutETag.Type);
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateCore_NullPk_SetsPk()
    {
        var ms = new MemoryStream();
        TestDocWithoutETag.pk = null!;
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.CreateItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type);
        Assert.Equal(PartitionKeyString, TestDocWithoutETag.pk);
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateCore_SetsCreationDate()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.CreateItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateCore(TestDocWithoutETag, TestDocWithoutETag.Type);
        Assert.True(Math.Abs(TestDocWithoutETag.CreationDate.Subtract(DateTime.UtcNow).TotalSeconds) < 5);
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateOrReplaceCore_NullItem_Throws() => Assert.Throws<ArgumentNullException>(() => CreateTestBatch().CreateOrReplaceCore<DbDoc>(null!, "asdf"));

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateOrReplaceCore_NullType_Throws() => Assert.Throws<ArgumentNullException>(() => CreateTestBatch().CreateOrReplaceCore(TestDocWithoutETag, null!));

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateOrReplaceCore_NullId_Throws()
    {
        TestDocWithoutETag.id = null!;
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateOrReplaceCore(TestDocWithoutETag, TestDocWithoutETag.Type));
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateOrReplaceCore_NonNullETag_Throws() => Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateOrReplaceCore(TestDocWithETag, TestDocWithETag.Type));

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateOrReplaceCore_WrongPK_Throws()
    {
        TestDocWithoutETag.pk += "a";
        Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateOrReplaceCore(TestDocWithoutETag, TestDocWithoutETag.Type));
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateOrReplaceCore_WrongType_Throws() => Assert.Throws<InvalidOperationException>(() => CreateTestBatch().CreateOrReplaceCore(TestDocWithoutETag, TestDocWithoutETag.Type + "a"));

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateOrReplaceCore_SetsIsEmptyFalse()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.UpsertItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateOrReplaceCore(TestDocWithoutETag, TestDocWithoutETag.Type);
        Assert.False(batch.IsEmpty);
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public async Task CreateOrReplaceCore_AfterExecute_Throws()
    {
        var batch = CreateTestBatch();
        await batch.ExecuteAsync();
        Assert.Throws<InvalidOperationException>(() => batch.CreateOrReplaceCore(TestDocWithoutETag, TestDocWithoutETag.Type));
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateOrReplaceCore_NullType_SetsType()
    {
        var ms = new MemoryStream();
        var type = TestDocWithoutETag.Type;
        TestDocWithoutETag.Type = null!;
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.UpsertItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateOrReplaceCore(TestDocWithoutETag, type);
        Assert.Equal(type, TestDocWithoutETag.Type);
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateOrReplaceCore_NullPk_SetsPk()
    {
        var ms = new MemoryStream();
        TestDocWithoutETag.pk = null!;
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.UpsertItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateOrReplaceCore(TestDocWithoutETag, TestDocWithoutETag.Type);
        Assert.Equal(PartitionKeyString, TestDocWithoutETag.pk);
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void CreateOrReplaceCore_SetsCreationDate()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockBatch.Setup(x => x.UpsertItemStream(ms, null)).Returns(MockBatch.Object).Verifiable();

        var batch = CreateTestBatch();
        batch.CreateOrReplaceCore(TestDocWithoutETag, TestDocWithoutETag.Type);
        Assert.True(Math.Abs(TestDocWithoutETag.CreationDate.Subtract(DateTime.UtcNow).TotalSeconds) < 5);
        MockSerializer.Verify();
        MockBatch.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public async Task ExecuteAsync_BatchSuccess_ReturnsTrue()
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
        var result = await batch.ExecuteAsync();

        Assert.True(result);
        mockResponse.Verify();
        mockOpResponse.Verify();
        MockSerializer.Verify();
        MockBatch.Verify();
    }
}

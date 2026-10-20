using System.Net;
using Microsoft.Azure.Cosmos;
using Moq;

namespace Test.Cosmogenesis.Core;

public class DbBaseTests
{
    class TestDb(Container container, DbSerializerBase serializer, bool isReadOnly, bool validateStateBeforeSave) : DbBase(container, serializer, isReadOnly, validateStateBeforeSave)
    {
        public new Task<T?> ReadByIdAsync<T>(string id, PartitionKey partitionKey, string type) where T : DbDoc => base.ReadByIdAsync<T>(id, partitionKey, type);
        public new Task<T?[]> ReadByIdsAsync<T>(IEnumerable<string> ids, PartitionKey partitionKey, string type) where T : DbDoc => base.ReadByIdsAsync<T>(ids, partitionKey, type);
        public new Task<DbDoc?> ReadByIdAsync(string id, PartitionKey partitionKey) => base.ReadByIdAsync(id, partitionKey);
        public new Task<DbDoc?[]> ReadByIdsAsync(IEnumerable<string> ids, PartitionKey partitionKey) => base.ReadByIdsAsync(ids, partitionKey);
        public new Task<CreateResult<T>> CreateItemAsync<T>(T item, string type, PartitionKey partitionKey, string partitionKeyString) where T : DbDoc => base.CreateItemAsync(item, type, partitionKey, partitionKeyString);
        public new Task<CreateOrReplaceResult<T>> CreateOrReplaceItemAsync<T>(T item, string type, PartitionKey partitionKey, string partitionKeyString, bool allowTtl) where T : DbDoc => base.CreateOrReplaceItemAsync(item, type, partitionKey, partitionKeyString, allowTtl);
        public new Task<ReadOrCreateResult<T>> ReadOrCreateItemAsync<T>(T item, string type, PartitionKey partitionKey, string partitionKeyString, bool tryCreateFirst) where T : DbDoc => base.ReadOrCreateItemAsync(item, type, partitionKey, partitionKeyString, tryCreateFirst);
        public new Task<DbConflictType?> DeleteItemAsync<T>(T item, PartitionKey partitionKey, string partitionKeyString) where T : DbDoc => base.DeleteItemAsync(item, partitionKey, partitionKeyString);
        protected override void ValidateContainerDefaultTtl(int? ttl) { }
    }
    public class ReadByIdsAsyncDb(Container container, DbSerializerBase serializer, bool isReadOnly, bool validateStateBeforeSave) : DbBase(container, serializer, isReadOnly, validateStateBeforeSave)
    {
        public new Task<T?[]> ReadByIdsAsync<T>(IEnumerable<string> ids, PartitionKey partitionKey, string type) where T : DbDoc => base.ReadByIdsAsync<T>(ids, partitionKey, type);
#pragma warning disable CS8609 // Nullability of reference types in return type doesn't match overridden member.
        protected sealed override Task<DbDoc?> ReadByIdAsync(string id, PartitionKey partitionKey) => MockReadByIdAsync(id, partitionKey);
        protected sealed override Task<DbDoc?[]> ReadByIdsAsync(IEnumerable<string> ids, PartitionKey partitionKey) => base.ReadByIdsAsync(ids, partitionKey);
        protected sealed override Task<T> ReadByIdAsync<T>(string id, PartitionKey partitionKey, string type) => MockReadByIdAsync<T>(id, partitionKey, type)!;
#pragma warning restore CS8609 // Nullability of reference types in return type doesn't match overridden member.
        public virtual Task<T?> MockReadByIdAsync<T>(string id, PartitionKey partitionKey, string type) where T : DbDoc => throw new NotImplementedException();
        public virtual Task<DbDoc?> MockReadByIdAsync(string id, PartitionKey partitionKey) => throw new NotImplementedException();
        protected override void ValidateContainerDefaultTtl(int? ttl) { }
    }

    readonly Mock<Container> MockContainer = new(MockBehavior.Strict);
    readonly Mock<DbSerializerBase> MockSerializer = new(MockBehavior.Strict);
    readonly Mock<ResponseMessage> MockResponseMessage = new(MockBehavior.Loose);
    const string PartitionKeyString = "testpk";
    const string Type = "testtype";
    readonly PartitionKey PartitionKey = new(PartitionKeyString);

    readonly TestDoc TestDocWithETag = new()
    {
        id = "asdf",
        pk = PartitionKeyString,
        Type = Type,
        _etag = "asdf"
    };
    readonly TestDoc TestDocWithoutETag = new()
    {
        id = "asdf",
        pk = PartitionKeyString,
        Type = Type
    };

    TestDb CreateDb(bool isReadOnly = false, bool validateStateBeforeSave = true) => new(MockContainer.Object, MockSerializer.Object, isReadOnly, validateStateBeforeSave);

    [Test]
    public async Task Ctor_NullContainer_Throws() => Assert.Throws<ArgumentNullException>(() => new TestDb(null!, MockSerializer.Object, false, true));

    [Test]
    public async Task Ctor_NullSerializer_Throws() => Assert.Throws<ArgumentNullException>(() => new TestDb(MockContainer.Object, null!, false, true));

    [Test]
    public async Task Ctor_ReadOnly_SetsProperty() => await Assert.That(CreateDb(true).IsReadOnly).IsTrue();

    [Test]
    public async Task Ctor_NotReadOnly_SetsProperty() => await Assert.That(CreateDb(false).IsReadOnly).IsFalse();

    [Test]
    public async Task ReadByIdAsync_NullId_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().ReadByIdAsync<TestDoc>(null!, PartitionKey, Type));

    [Test]
    public async Task ReadByIdAsync_NullType_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().ReadByIdAsync<TestDoc>("id", PartitionKey, null!));

    [Test]
    public async Task ReadByIdsAsync_NullIds_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().ReadByIdsAsync<TestDoc>(null!, PartitionKey, Type));

    [Test]
    public async Task ReadByIdsAsync_NullType_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().ReadByIdsAsync<TestDoc>(["id"], PartitionKey, null!));

    [Test]
    public async Task ReadByIdsAsync_NullId_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().ReadByIdsAsync<TestDoc>(["id", null!], PartitionKey, Type));


    [Test]
    public async Task CreateItemAsync_NullItem_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().CreateItemAsync<TestDoc>(null!, Type, PartitionKey, PartitionKeyString));

    [Test]
    public async Task CreateItemAsync_NullType_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().CreateItemAsync(TestDocWithoutETag, null!, PartitionKey, PartitionKeyString));

    [Test]
    public async Task CreateItemAsync_NullPk_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().CreateItemAsync(TestDocWithoutETag, Type, PartitionKey, null!));

    [Test]
    public async Task CreateItemAsync_ReadOnly_Throws() => await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb(true).CreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString));

    [Test]
    public async Task CreateItemAsync_NullItemId_Throws()
    {
        TestDocWithoutETag.id = null!;
        await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().CreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString));
    }

    [Test]
    public async Task CreateItemAsync_NonNullItemETag_Throws() => await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().CreateItemAsync(TestDocWithETag, Type, PartitionKey, PartitionKeyString));

    [Test]
    public async Task CreateItemAsync_WrongItemPK_Throws()
    {
        TestDocWithoutETag.pk += "a";
        await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().CreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString));
    }

    [Test]
    public async Task CreateItemAsync_WrongItemType_Throws()
    {
        var doc = new TestDoc
        {
            id = TestDocWithETag.id,
            pk = PartitionKeyString,
            Type = TestDocWithETag.Type + "a"
        };
        await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().CreateItemAsync(doc, Type, PartitionKey, PartitionKeyString));
    }


    [Test]
    public async Task CreateOrReplaceItemAsync_NullItem_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().CreateOrReplaceItemAsync<TestDoc>(null!, Type, PartitionKey, PartitionKeyString, false));

    [Test]
    public async Task CreateOrReplaceItemAsync_NullType_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().CreateOrReplaceItemAsync(TestDocWithoutETag, null!, PartitionKey, PartitionKeyString, false));

    [Test]
    public async Task CreateOrReplaceItemAsync_NullPk_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().CreateOrReplaceItemAsync(TestDocWithoutETag, Type, PartitionKey, null!, false));

    [Test]
    public async Task CreateOrReplaceItemAsync_ReadOnly_Throws() => await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb(true).CreateOrReplaceItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false));

    [Test]
    public async Task CreateOrReplaceItemAsync_NullItemId_Throws()
    {
        TestDocWithoutETag.id = null!;
        await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().CreateOrReplaceItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false));
    }

    [Test]
    public async Task CreateOrReplaceItemAsync_NonNullItemETag_Throws() => await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().CreateOrReplaceItemAsync(TestDocWithETag, Type, PartitionKey, PartitionKeyString, false));

    [Test]
    public async Task CreateOrReplaceItemAsync_WrongItemPK_Throws()
    {
        TestDocWithoutETag.pk += "a";
        await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().CreateOrReplaceItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false));
    }

    [Test]
    public async Task CreateOrReplaceItemAsync_WrongItemType_Throws()
    {
        var doc = new TestDoc
        {
            id = TestDocWithETag.id,
            pk = PartitionKeyString,
            Type = TestDocWithETag.Type + "a"
        };
        await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().CreateOrReplaceItemAsync(doc, Type, PartitionKey, PartitionKeyString, false));
    }



    [Test]
    public async Task ReadOrCreateItemAsync_NullItem_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().ReadOrCreateItemAsync<TestDoc>(null!, Type, PartitionKey, PartitionKeyString, true));

    [Test]
    public async Task ReadOrCreateItemAsync_NullType_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().ReadOrCreateItemAsync(TestDocWithoutETag, null!, PartitionKey, PartitionKeyString, true));

    [Test]
    public async Task ReadOrCreateItemAsync_NullPk_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, null!, true));

    [Test]
    public async Task ReadOrCreateItemAsync_ReadOnly_Throws() => await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb(true).ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, true));

    [Test]
    public async Task ReadOrCreateItemAsync_NullItemId_Throws()
    {
        TestDocWithoutETag.id = null!;
        await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, true));
    }

    [Test]
    public async Task ReadOrCreateItemAsync_NonNullItemETag_Throws() => await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().ReadOrCreateItemAsync(TestDocWithETag, Type, PartitionKey, PartitionKeyString, true));

    [Test]
    public async Task ReadOrCreateItemAsync_WrongItemPK_Throws()
    {
        TestDocWithoutETag.pk += "a";
        await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, true));
    }

    [Test]
    public async Task ReadOrCreateItemAsync_WrongItemType_Throws()
    {
        var doc = new TestDoc
        {
            id = TestDocWithETag.id,
            pk = PartitionKeyString,
            Type = TestDocWithETag.Type + "a"
        };
        await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().ReadOrCreateItemAsync(doc, Type, PartitionKey, PartitionKeyString, true));
    }


    [Test]
    public async Task ReplaceItemAsync_NullItem_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().ReplaceItemAsync<TestDoc>(null!, Type, PartitionKey, PartitionKeyString, false));

    [Test]
    public async Task ReplaceItemAsync_NullType_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().ReplaceItemAsync(TestDocWithETag, null!, PartitionKey, PartitionKeyString, false));

    [Test]
    public async Task ReplaceItemAsync_NullPk_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().ReplaceItemAsync(TestDocWithETag, Type, PartitionKey, null!, false));

    [Test]
    public async Task ReplaceItemAsync_ReadOnly_Throws() => await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb(true).ReplaceItemAsync(TestDocWithETag, Type, PartitionKey, PartitionKeyString, false));

    [Test]
    public async Task ReplaceItemAsync_NullItemId_Throws()
    {
        TestDocWithETag.id = null!;
        await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().ReplaceItemAsync(TestDocWithETag, Type, PartitionKey, PartitionKeyString, false));
    }

    [Test]
    public async Task ReplaceItemAsync_NullItemETag_Throws() => await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().ReplaceItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false));

    [Test]
    public async Task ReplaceItemAsync_WrongItemPK_Throws()
    {
        TestDocWithETag.pk += "a";
        await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().ReplaceItemAsync(TestDocWithETag, Type, PartitionKey, PartitionKeyString, false));
    }

    [Test]
    public async Task ReplaceItemAsync_WrongItemType_Throws()
    {
        var doc = new TestDoc
        {
            id = TestDocWithETag.id,
            pk = PartitionKeyString,
            Type = TestDocWithETag.Type + "a",
            _etag = TestDocWithETag._etag
        };
        await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().ReplaceItemAsync(doc, Type, PartitionKey, PartitionKeyString, false));
    }



    [Test]
    public async Task DeleteItemAsync_NullItem_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().DeleteItemAsync<TestDoc>(null!, PartitionKey, PartitionKeyString));

    [Test]
    public async Task DeleteItemAsync_NullPk_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().DeleteItemAsync(TestDocWithETag, PartitionKey, null!));

    [Test]
    public async Task DeleteItemAsync_ReadOnly_Throws() => await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb(true).DeleteItemAsync(TestDocWithETag, PartitionKey, PartitionKeyString));

    [Test]
    public async Task DeleteItemAsync_NullItemId_Throws()
    {
        TestDocWithETag.id = null!;
        await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().DeleteItemAsync(TestDocWithETag, PartitionKey, PartitionKeyString));
    }

    [Test]
    public async Task DeleteItemAsync_NullItemETag_Throws() => await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().DeleteItemAsync(TestDocWithoutETag, PartitionKey, PartitionKeyString));

    [Test]
    public async Task DeleteItemAsync_WrongItemPK_Throws()
    {
        TestDocWithETag.pk += "a";
        await Assert.ThrowsAsync<InvalidOperationException>(() => CreateDb().DeleteItemAsync(TestDocWithETag, PartitionKey, PartitionKeyString));
    }

    [Test]
    public async Task ExecuteQueryAsync_NullQuery_Throws() => await Assert.ThrowsAsync<ArgumentNullException>(() => CreateDb().ExecuteQueryAsync<TestDoc>(null!, default).ToListAsync().AsTask());




    [Test]
    public async Task ReadByIdAsync_NotFound_ReturnsNull()
    {
        MockResponseMessage.Setup(x => x.IsSuccessStatusCode).Returns(false).Verifiable();
        MockResponseMessage.Setup(x => x.StatusCode).Returns(HttpStatusCode.NotFound).Verifiable();
        MockContainer.Setup(x => x.ReadItemStreamAsync("id", PartitionKey, null, It.IsAny<CancellationToken>())).Returns(Task.FromResult(MockResponseMessage.Object)).Verifiable();

        var result = await CreateDb().ReadByIdAsync<TestDoc>("id", PartitionKey, Type);

        Assert.Null(result);
        MockResponseMessage.Verify();
        MockContainer.Verify();
    }

    [Test]
    public async Task ReadByIdAsync_429_Throws()
    {
        MockResponseMessage.Setup(x => x.IsSuccessStatusCode).Returns(false).Verifiable();
        MockResponseMessage.Setup(x => x.StatusCode).Returns((HttpStatusCode)429).Verifiable();
        MockContainer.Setup(x => x.ReadItemStreamAsync("id", PartitionKey, null, It.IsAny<CancellationToken>())).Returns(Task.FromResult(MockResponseMessage.Object)).Verifiable();

        await Assert.ThrowsAsync<DbOverloadedException>(() => CreateDb().ReadByIdAsync<TestDoc>("id", PartitionKey, Type));
    }

    [Test]
    public async Task ReadByIdAsync_OtherStatus_Throws()
    {
        MockResponseMessage.Setup(x => x.IsSuccessStatusCode).Returns(false).Verifiable();
        MockResponseMessage.Setup(x => x.StatusCode).Returns(HttpStatusCode.BadRequest).Verifiable();
        MockContainer.Setup(x => x.ReadItemStreamAsync("id", PartitionKey, null, It.IsAny<CancellationToken>())).Returns(Task.FromResult(MockResponseMessage.Object)).Verifiable();

        await Assert.ThrowsAsync<DbUnknownStatusCodeException>(() => CreateDb().ReadByIdAsync<TestDoc>("id", PartitionKey, Type));
    }



    [Test]
    public async Task ReadByIdsAsync_NoIds_ReturnsEmptySynchronously()
    {
        var result = CreateDb().ReadByIdsAsync<TestDoc>([], PartitionKey, Type);

        await Assert.That(result.IsCompletedSuccessfully).IsTrue();
        await Assert.That(result.Result).IsEmpty();
    }

    [Test]
    public async Task ReadByIdsAsync_2_CallsReadByIdAsync()
    {
        var mockDb = new Mock<ReadByIdsAsyncDb>(MockBehavior.Strict, MockContainer.Object, MockSerializer.Object, false, true);
        var docs = new[]
        {
            TestDocWithETag,
            new TestDoc
            {
                id = "Asdffff",
                pk = PartitionKeyString,
                _etag = "Asdf",
                _ts = 1,
                Type = Type
            }
        };
        foreach (var doc in docs)
        {
            mockDb.Setup(x => x.MockReadByIdAsync(doc.id, PartitionKey)).Returns(Task.FromResult<DbDoc>(doc)!).Verifiable();
        }

        var result = await mockDb.Object.ReadByIdsAsync<TestDoc>(docs.Select(x => x.id), PartitionKey, Type);

        await Assert.That(result as TestDoc[]).IsEquivalentTo(docs);
        mockDb.Verify();
    }

    static async IAsyncEnumerable<T> AsAsyncEnumerable<T>(IQueryable<T> query)
    {
        await Task.CompletedTask;
        var results = query.ToList();
        foreach (var result in results)
        {
            yield return result;
        }
    }

    [Test]
    public async Task ReadByIdsAsync_10_CallsQuery()
    {
        var mockDb = new Mock<ReadByIdsAsyncDb>(MockBehavior.Strict, MockContainer.Object, MockSerializer.Object, false, true);
        var docs = Enumerable.Range(0, 10).Select(x => new TestDoc
        {
            id = $"Asdffff{x}",
            pk = PartitionKeyString,
            _etag = $"Asdf{x}",
            _ts = 1,
            Type = Type
        }).ToList();
        var fail = false;
        mockDb.Setup(x => x.Container).CallBase();
        MockContainer
            .Setup(x => x.GetItemLinqQueryable<DbDoc>(false, null, It.IsAny<QueryRequestOptions>(), It.IsAny<CosmosLinqSerializerOptions>()))
            .Returns((bool x, string? s, QueryRequestOptions? o, CosmosLinqSerializerOptions? so) =>
            {
                if (PartitionKey != o?.PartitionKey) { fail = true; }
                return docs.AsQueryable().OrderBy(x => RandomHelper.GetRandomInt32());
            })
            .Verifiable();
        mockDb
            .Setup(x => x.ExecuteQueryAsync(It.IsAny<IQueryable<DbDoc>>(), It.IsAny<CancellationToken>()))
            .Returns((IQueryable<DbDoc> q, CancellationToken c) => AsAsyncEnumerable(q))
            .Verifiable();

        await Assert.That(fail).IsFalse();

        var result = await mockDb.Object.ReadByIdsAsync<TestDoc>(docs.Select(x => x.id), PartitionKey, Type);

        await Assert.That(result as TestDoc[]).IsEquivalentTo(docs);
        mockDb.Verify();
        MockContainer.Verify();
    }

    [Test]
    public async Task ReadByIdsAsync_Segmented_CallsQuerySegmented()
    {
        const int Segments = 3;
        var mockDb = new Mock<ReadByIdsAsyncDb>(MockBehavior.Strict, MockContainer.Object, MockSerializer.Object, false, true);
        var docs = Enumerable.Range(0, DbBase.MaxIdsPerQuery * Segments).Select(x => new TestDoc
        {
            id = $"Asdffff{x}",
            pk = PartitionKeyString,
            _etag = $"Asdf{x}",
            _ts = 1,
            Type = Type
        }).ToDictionary(x => x.id);
        var count = 0;
        var fail = false;
        mockDb.Setup(x => x.Container).CallBase();
        MockContainer
            .Setup(x => x.GetItemLinqQueryable<DbDoc>(false, null, It.IsAny<QueryRequestOptions>(), It.IsAny<CosmosLinqSerializerOptions>()))
            .Returns((bool x, string? s, QueryRequestOptions? o, CosmosLinqSerializerOptions? so) =>
            {
                if (PartitionKey != o?.PartitionKey) { fail = true; }
                ++count;
                return docs.Values.AsQueryable().OrderBy(x => RandomHelper.GetRandomInt32());
            })
            .Verifiable();
        mockDb
            .Setup(x => x.ExecuteQueryAsync(It.IsAny<IQueryable<DbDoc>>(), It.IsAny<CancellationToken>()))
            .Returns((IQueryable<DbDoc> q, CancellationToken c) => AsAsyncEnumerable(q))
            .Verifiable();

        await Assert.That(fail).IsFalse();

        var result = await mockDb.Object.ReadByIdsAsync<TestDoc>(docs.Keys, PartitionKey, Type);

        await Assert.That(result.ToArray() as TestDoc[]).IsEquivalentTo([.. docs.Values]);
        await Assert.That(count).IsEqualTo(Segments);
        mockDb.Verify();
        MockContainer.Verify();
    }

    [Test]
    public async Task CreateItemAsync_Creates()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.IsSuccessStatusCode).Returns(true).Verifiable();
        MockResponseMessage.Setup(x => x.Content).Returns(ms).Verifiable();
        MockSerializer.Setup(x => x.FromStream<TestDoc>(ms)).Returns(TestDocWithETag).Verifiable();
        MockContainer.Setup(x => x.CreateItemStreamAsync(ms, PartitionKey, null, It.IsAny<CancellationToken>())).Returns(Task.FromResult(MockResponseMessage.Object)).Verifiable();

        var result = await CreateDb().CreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString);

        Assert.Null(result.Conflict);
        await Assert.That(result.Document).IsSameReferenceAs(TestDocWithETag);
        MockSerializer.Verify();
        MockResponseMessage.Verify();
        MockContainer.Verify();
    }

    [Test]
    public async Task CreateItemAsync_NullPK_SetsPK()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.IsSuccessStatusCode).Returns(true).Verifiable();
        MockResponseMessage.Setup(x => x.Content).Returns(ms).Verifiable();
        MockSerializer.Setup(x => x.FromStream<TestDoc>(ms)).Returns(TestDocWithETag).Verifiable();
        MockContainer.Setup(x => x.CreateItemStreamAsync(ms, PartitionKey, null, It.IsAny<CancellationToken>())).Returns(Task.FromResult(MockResponseMessage.Object)).Verifiable();
        TestDocWithoutETag.pk = null!;

        await CreateDb().CreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString);

        await Assert.That(TestDocWithoutETag.pk).IsEqualTo(PartitionKeyString);
    }

    [Test]
    public async Task CreateItemAsync_NullType_SetsType()
    {
        var doc = new TestDoc
        {
            id = TestDocWithETag.id,
            pk = PartitionKeyString
        };
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(doc)).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.IsSuccessStatusCode).Returns(true).Verifiable();
        MockResponseMessage.Setup(x => x.Content).Returns(ms).Verifiable();
        MockSerializer.Setup(x => x.FromStream<TestDoc>(ms)).Returns(TestDocWithETag).Verifiable();
        MockContainer.Setup(x => x.CreateItemStreamAsync(ms, PartitionKey, null, It.IsAny<CancellationToken>())).Returns(Task.FromResult(MockResponseMessage.Object)).Verifiable();

        await CreateDb().CreateItemAsync(doc, Type, PartitionKey, PartitionKeyString);

        await Assert.That(doc.Type).IsEqualTo(Type);
    }

    [Test]
    public async Task CreateItemAsync_SetsCreationDate()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.IsSuccessStatusCode).Returns(true).Verifiable();
        MockResponseMessage.Setup(x => x.Content).Returns(ms).Verifiable();
        MockSerializer.Setup(x => x.FromStream<TestDoc>(ms)).Returns(TestDocWithETag).Verifiable();
        MockContainer.Setup(x => x.CreateItemStreamAsync(ms, PartitionKey, null, It.IsAny<CancellationToken>())).Returns(Task.FromResult(MockResponseMessage.Object)).Verifiable();

        await CreateDb().CreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString);

        await Assert.That(Math.Abs(TestDocWithoutETag.CreationDate.Subtract(DateTime.UtcNow).TotalSeconds) < 5).IsTrue();
    }

    [Test]
    public async Task CreateOrReplaceItemAsync_Upserts()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.IsSuccessStatusCode).Returns(true).Verifiable();
        MockResponseMessage.Setup(x => x.Content).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.StatusCode).Returns(HttpStatusCode.OK);
        MockSerializer.Setup(x => x.FromStream<TestDoc>(ms)).Returns(TestDocWithETag).Verifiable();
        MockContainer.Setup(x => x.UpsertItemStreamAsync(ms, PartitionKey, null, It.IsAny<CancellationToken>())).Returns(Task.FromResult(MockResponseMessage.Object)).Verifiable();

        var result = await CreateDb().CreateOrReplaceItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false);

        await Assert.That(result.Document).IsSameReferenceAs(TestDocWithETag);
        MockSerializer.Verify();
        MockResponseMessage.Verify();
        MockContainer.Verify();
    }

    [Test]
    public async Task CreateOrReplaceItemAsync_NullPK_SetsPK()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.IsSuccessStatusCode).Returns(true).Verifiable();
        MockResponseMessage.Setup(x => x.Content).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.StatusCode).Returns(HttpStatusCode.OK);
        MockSerializer.Setup(x => x.FromStream<TestDoc>(ms)).Returns(TestDocWithETag).Verifiable();
        MockContainer.Setup(x => x.UpsertItemStreamAsync(ms, PartitionKey, null, It.IsAny<CancellationToken>())).Returns(Task.FromResult(MockResponseMessage.Object)).Verifiable();
        TestDocWithoutETag.pk = null!;

        await CreateDb().CreateOrReplaceItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false);

        await Assert.That(TestDocWithoutETag.pk).IsEqualTo(PartitionKeyString);
    }

    [Test]
    public async Task CreateOrReplaceItemAsync_NullType_SetsType()
    {
        var doc = new TestDoc
        {
            id = TestDocWithETag.id,
            pk = PartitionKeyString
        };
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(doc)).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.IsSuccessStatusCode).Returns(true).Verifiable();
        MockResponseMessage.Setup(x => x.Content).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.StatusCode).Returns(HttpStatusCode.OK);
        MockSerializer.Setup(x => x.FromStream<TestDoc>(ms)).Returns(TestDocWithETag).Verifiable();
        MockContainer.Setup(x => x.UpsertItemStreamAsync(ms, PartitionKey, null, It.IsAny<CancellationToken>())).Returns(Task.FromResult(MockResponseMessage.Object)).Verifiable();

        await CreateDb().CreateOrReplaceItemAsync(doc, Type, PartitionKey, PartitionKeyString, false);

        await Assert.That(doc.Type).IsEqualTo(Type);
    }

    [Test]
    public async Task CreateOrReplaceItemAsync_SetsCreationDate()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.IsSuccessStatusCode).Returns(true).Verifiable();
        MockResponseMessage.Setup(x => x.Content).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.StatusCode).Returns(HttpStatusCode.OK);
        MockSerializer.Setup(x => x.FromStream<TestDoc>(ms)).Returns(TestDocWithETag).Verifiable();
        MockContainer.Setup(x => x.UpsertItemStreamAsync(ms, PartitionKey, null, It.IsAny<CancellationToken>())).Returns(Task.FromResult(MockResponseMessage.Object)).Verifiable();

        await CreateDb().CreateOrReplaceItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false);

        await Assert.That(Math.Abs(TestDocWithoutETag.CreationDate.Subtract(DateTime.UtcNow).TotalSeconds) < 5).IsTrue();
    }


    [Test]
    public async Task ReadOrCreateItemAsync_Exists_ReadFirst()
    {
        var mockDb = new Mock<ReadByIdsAsyncDb>(MockBehavior.Strict, MockContainer.Object, MockSerializer.Object, false, true);
        mockDb.Setup(x => x.IsReadOnly).Returns(false).Verifiable();
        mockDb.Setup(x => x.MockReadByIdAsync<TestDoc>(TestDocWithoutETag.id, PartitionKey, Type)).Returns(Task.FromResult(TestDocWithoutETag)!).Verifiable();
        mockDb.Setup(x => x.ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false)).CallBase();

        var result = await mockDb.Object.ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false);

        await Assert.That(result.AlreadyExisted).IsTrue();
        await Assert.That(result.Document).IsSameReferenceAs(TestDocWithoutETag);
        mockDb.Verify();
    }

    [Test]
    public async Task ReadOrCreateItemAsync_NullPK_SetsPK()
    {
        var mockDb = new Mock<ReadByIdsAsyncDb>(MockBehavior.Strict, MockContainer.Object, MockSerializer.Object, false, true);
        mockDb.Setup(x => x.IsReadOnly).Returns(false).Verifiable();
        mockDb.Setup(x => x.MockReadByIdAsync<TestDoc>(TestDocWithoutETag.id, PartitionKey, Type)).Returns(Task.FromResult(TestDocWithoutETag)!).Verifiable();
        mockDb.Setup(x => x.ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false)).CallBase();
        TestDocWithoutETag.pk = null!;

        await mockDb.Object.ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false);

        await Assert.That(TestDocWithoutETag.pk).IsEqualTo(PartitionKeyString);
    }

    [Test]
    public async Task ReadOrCreateItemAsync_NullType_SetsType()
    {
        var doc = new TestDoc
        {
            id = TestDocWithoutETag.id,
            pk = PartitionKeyString
        };
        var mockDb = new Mock<ReadByIdsAsyncDb>(MockBehavior.Strict, MockContainer.Object, MockSerializer.Object, false, true);
        mockDb.Setup(x => x.IsReadOnly).Returns(false).Verifiable();
        mockDb.Setup(x => x.MockReadByIdAsync<TestDoc>(TestDocWithoutETag.id, PartitionKey, Type)).Returns(Task.FromResult(TestDocWithoutETag)!).Verifiable();
        mockDb.Setup(x => x.ReadOrCreateItemAsync(doc, Type, PartitionKey, PartitionKeyString, false)).CallBase();

        await mockDb.Object.ReadOrCreateItemAsync(doc, Type, PartitionKey, PartitionKeyString, false);

        await Assert.That(doc.Type).IsEqualTo(Type);
    }

    [Test]
    public async Task ReadOrCreateItemAsync_SetsCreationDate()
    {
        var mockDb = new Mock<ReadByIdsAsyncDb>(MockBehavior.Strict, MockContainer.Object, MockSerializer.Object, false, true);
        mockDb.Setup(x => x.IsReadOnly).Returns(false).Verifiable();
        mockDb.Setup(x => x.MockReadByIdAsync<TestDoc>(TestDocWithoutETag.id, PartitionKey, Type)).Returns(Task.FromResult(TestDocWithoutETag)!).Verifiable();
        mockDb.Setup(x => x.ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false)).CallBase();

        await mockDb.Object.ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false);

        await Assert.That(Math.Abs(TestDocWithoutETag.CreationDate.Subtract(DateTime.UtcNow).TotalSeconds) < 5).IsTrue();
    }

    [Test]
    public async Task ReadOrCreateItemAsync_DoesntExist_CreateFirst()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.IsSuccessStatusCode).Returns(true).Verifiable();
        MockResponseMessage.Setup(x => x.StatusCode).Returns(HttpStatusCode.OK);
        MockResponseMessage.Setup(x => x.Content).Returns(ms).Verifiable();
        MockContainer.Setup(x => x.CreateItemStreamAsync(ms, PartitionKey, null, It.IsAny<CancellationToken>())).Returns(Task.FromResult(MockResponseMessage.Object)).Verifiable();
        MockSerializer.Setup(x => x.FromStream<TestDoc>(ms)).Returns(TestDocWithETag).Verifiable();
        var mockDb = new Mock<ReadByIdsAsyncDb>(MockBehavior.Strict, MockContainer.Object, MockSerializer.Object, false, true);
        mockDb.Setup(x => x.IsReadOnly).Returns(false).Verifiable();
        mockDb.Setup(x => x.Container).CallBase();
        mockDb.Setup(x => x.Serializer).CallBase();
        mockDb.Setup(x => x.ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, true)).CallBase();

        var result = await mockDb.Object.ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, true);

        await Assert.That(result.AlreadyExisted).IsFalse();
        await Assert.That(result.Document).IsSameReferenceAs(TestDocWithETag);
        mockDb.Verify();
        MockSerializer.Verify();
    }

    [Test]
    public async Task ReadOrCreateItemAsync_DoesntExist_ReadFirst()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.IsSuccessStatusCode).Returns(true).Verifiable();
        MockResponseMessage.Setup(x => x.StatusCode).Returns(HttpStatusCode.OK);
        MockResponseMessage.Setup(x => x.Content).Returns(ms).Verifiable();
        MockContainer.Setup(x => x.CreateItemStreamAsync(ms, PartitionKey, null, It.IsAny<CancellationToken>())).Returns(Task.FromResult(MockResponseMessage.Object)).Verifiable();
        MockSerializer.Setup(x => x.FromStream<TestDoc>(ms)).Returns(TestDocWithETag).Verifiable();
        var mockDb = new Mock<ReadByIdsAsyncDb>(MockBehavior.Strict, MockContainer.Object, MockSerializer.Object, false, true);
        mockDb.Setup(x => x.IsReadOnly).Returns(false).Verifiable();
        mockDb.Setup(x => x.Container).CallBase();
        mockDb.Setup(x => x.Serializer).CallBase();
        mockDb.Setup(x => x.MockReadByIdAsync<TestDoc>(TestDocWithoutETag.id, PartitionKey, Type)).Returns(Task.FromResult((TestDoc?)null)).Verifiable();
        mockDb.Setup(x => x.ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false)).CallBase();

        var result = await mockDb.Object.ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, false);

        await Assert.That(result.AlreadyExisted).IsFalse();
        await Assert.That(result.Document).IsSameReferenceAs(TestDocWithETag);
        mockDb.Verify();
        MockSerializer.Verify();
    }

    [Test]
    public async Task ReadOrCreateItemAsync_Exists_CreateFirst()
    {
        var ms = new MemoryStream();
        MockSerializer.Setup(x => x.ToStream(TestDocWithoutETag)).Returns(ms).Verifiable();
        MockResponseMessage.Setup(x => x.IsSuccessStatusCode).Returns(false).Verifiable();
        MockResponseMessage.Setup(x => x.StatusCode).Returns(HttpStatusCode.Conflict).Verifiable();
        MockContainer.Setup(x => x.CreateItemStreamAsync(ms, PartitionKey, null, It.IsAny<CancellationToken>())).Returns(Task.FromResult(MockResponseMessage.Object)).Verifiable();
        var mockDb = new Mock<ReadByIdsAsyncDb>(MockBehavior.Strict, MockContainer.Object, MockSerializer.Object, false, true);
        mockDb.Setup(x => x.IsReadOnly).Returns(false).Verifiable();
        mockDb.Setup(x => x.Container).CallBase();
        mockDb.Setup(x => x.Serializer).CallBase();
        mockDb.Setup(x => x.MockReadByIdAsync<TestDoc>(TestDocWithoutETag.id, PartitionKey, Type)).Returns(Task.FromResult(TestDocWithETag)!).Verifiable();
        mockDb.Setup(x => x.ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, true)).CallBase();

        var result = await mockDb.Object.ReadOrCreateItemAsync(TestDocWithoutETag, Type, PartitionKey, PartitionKeyString, true);

        await Assert.That(result.AlreadyExisted).IsTrue();
        await Assert.That(result.Document).IsSameReferenceAs(TestDocWithETag);
        mockDb.Verify();
        MockSerializer.Verify();
    }
}

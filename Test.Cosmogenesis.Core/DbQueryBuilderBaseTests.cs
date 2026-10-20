using Microsoft.Azure.Cosmos;
using Moq;

namespace Test.Cosmogenesis.Core;

public class DbQueryBuilderBaseTests
{
    class TestQueryBuilder(DbBase dbBase, PartitionKey? partitionKey) : DbQueryBuilderBase(dbBase, partitionKey)
    {
        public new IQueryable<T> BuildQueryByType<T>(string type) where T : DbDoc => base.BuildQueryByType<T>(type);
        public new IQueryable<T> BuildQueryByTypes<T>(string[] types) where T : DbDoc => base.BuildQueryByTypes<T>(types);
    }

    readonly Mock<DbBase> MockDb = new(MockBehavior.Strict);

    [Test]
    public async Task Ctor_NullDb_Throws() => Assert.Throws<ArgumentNullException>(() => new TestQueryBuilder(null!, new PartitionKey("asdf")));

    [Test]
    public async Task Ctor_NullPartitionKey_DoesNotThrow() => _ = new TestQueryBuilder(MockDb.Object, null);

    [Test]
    public async Task BuildQueryByType_Null_Throws() => Assert.Throws<ArgumentNullException>(() => new TestQueryBuilder(MockDb.Object, null).BuildQueryByType<TestDoc>(null!));

    [Test]
    public async Task BuildQueryByTypes_Null_Throws() => Assert.Throws<ArgumentNullException>(() => new TestQueryBuilder(MockDb.Object, null).BuildQueryByTypes<TestDoc>(null!));

    [Test]
    public async Task BuildQueryByType_Type_NoPartition_CreatesQuery()
    {
        var items = new List<TestDoc>
        {
            TestDoc.Instance,
            new() { Type = "asdf" },
            TestDoc.Instance
        };
        var q = items.AsQueryable().OrderBy(x => true);
        MockDb
            .Setup(x => x.Container.GetItemLinqQueryable<TestDoc>(false, null, It.IsAny<QueryRequestOptions>(), It.IsAny<CosmosLinqSerializerOptions>()))
            .Returns((bool x, string y, QueryRequestOptions o, CosmosLinqSerializerOptions so) =>
            {
                Assert.Null(o.PartitionKey);
                return q;
            })
            .Verifiable();

        var result = new TestQueryBuilder(MockDb.Object, null).BuildQueryByType<TestDoc>("asdf").ToList();

        await Assert.That(result).HasSingleItem();
        await Assert.That(items[1]).IsSameReferenceAs(result[0]);
        MockDb.Verify();
    }

    [Test]
    public async Task BuildQueryByTypes_Types_NoPartition_CreatesQuery()
    {
        var items = new List<TestDoc>
        {
            TestDoc.Instance,
            new() { Type = "asdf" },
            TestDoc.Instance
        };
        var q = items.AsQueryable().OrderBy(x => true);
        MockDb
            .Setup(x => x.Container.GetItemLinqQueryable<TestDoc>(false, null, It.IsAny<QueryRequestOptions>(), It.IsAny<CosmosLinqSerializerOptions>()))
            .Returns((bool x, string y, QueryRequestOptions o, CosmosLinqSerializerOptions so) =>
            {
                Assert.Null(o.PartitionKey);
                return q;
            })
            .Verifiable();

        var result = new TestQueryBuilder(MockDb.Object, null).BuildQueryByTypes<TestDoc>(["asdf", "asdf2"]).ToList();

        await Assert.That(result).HasSingleItem();
        await Assert.That(items[1]).IsSameReferenceAs(result[0]);
        MockDb.Verify();
    }

    [Test]
    public async Task BuildQueryByType_Type_WithPartition_CreatesQuery()
    {
        var items = new List<TestDoc>
        {
            TestDoc.Instance,
            new() { Type = "asdf" },
            TestDoc.Instance
        };
        var q = items.AsQueryable().OrderBy(x => true);
        var pk = new PartitionKey("xxxx");
        var fail = false;
        MockDb
            .Setup(x => x.Container.GetItemLinqQueryable<TestDoc>(false, null, It.IsAny<QueryRequestOptions>(), It.IsAny<CosmosLinqSerializerOptions>()))
            .Returns((bool x, string y, QueryRequestOptions o, CosmosLinqSerializerOptions so) =>
            {
                if (o.PartitionKey != pk) { fail = true; }
                return q;
            })
            .Verifiable();

        await Assert.That(fail).IsFalse();

        var result = new TestQueryBuilder(MockDb.Object, pk).BuildQueryByType<TestDoc>("asdf").ToList();

        await Assert.That(result).HasSingleItem();
        await Assert.That(items[1]).IsSameReferenceAs(result[0]);
        MockDb.Verify();
    }

    [Test]
    public async Task BuildQueryByTypes_Types_WithPartition_CreatesQuery()
    {
        var items = new List<TestDoc>
        {
            TestDoc.Instance,
            new() { Type = "asdf" },
            TestDoc.Instance
        };
        var q = items.AsQueryable().OrderBy(x => true);
        var pk = new PartitionKey("xxxx");
        var fail = false;
        MockDb
            .Setup(x => x.Container.GetItemLinqQueryable<TestDoc>(false, null, It.IsAny<QueryRequestOptions>(), It.IsAny<CosmosLinqSerializerOptions>()))
            .Returns((bool x, string y, QueryRequestOptions o, CosmosLinqSerializerOptions so) =>
            {
                if (o.PartitionKey != pk) { fail = true; }
                return q;
            })
            .Verifiable();

        await Assert.That(fail).IsFalse();

        var result = new TestQueryBuilder(MockDb.Object, pk).BuildQueryByTypes<TestDoc>(["asdf", "asdf2"]).ToList();

        await Assert.That(result).HasSingleItem();
        await Assert.That(items[1]).IsSameReferenceAs(result[0]);
        MockDb.Verify();
    }

    [Test]
    public async Task Dynamic_NoPartition_CreatesQuery()
    {
        var items = new List<IDictionary<string, object>>
        {
            new Dictionary<string, object>()
        };
        var q = items.AsQueryable().OrderBy(x => true);
        MockDb
            .Setup(x => x.Container.GetItemLinqQueryable<IDictionary<string, object>>(false, null, It.IsAny<QueryRequestOptions>(), It.IsAny<CosmosLinqSerializerOptions>()))
            .Returns((bool x, string y, QueryRequestOptions o, CosmosLinqSerializerOptions so) =>
            {
                Assert.Null(o.PartitionKey);
                return q;
            })
            .Verifiable();

        var result = new TestQueryBuilder(MockDb.Object, null).Dynamic().ToList();

        await Assert.That(result).HasSingleItem();
        await Assert.That(items[0]).IsSameReferenceAs(result[0]);
        MockDb.Verify();
    }

    [Test]
    public async Task Dynamic_WithPartition_CreatesQuery()
    {
        var items = new List<IDictionary<string, object>>
        {
            new Dictionary<string, object>()
        };
        var q = items.AsQueryable().OrderBy(x => true);
        var pk = new PartitionKey("xxxx");
        var fail = false;
        MockDb
            .Setup(x => x.Container.GetItemLinqQueryable<IDictionary<string, object>>(false, null, It.IsAny<QueryRequestOptions>(), It.IsAny<CosmosLinqSerializerOptions>()))
            .Returns((bool x, string y, QueryRequestOptions o, CosmosLinqSerializerOptions so) =>
            {
                if (o.PartitionKey != pk) { fail = true; }
                return q;
            })
            .Verifiable();

        await Assert.That(fail).IsFalse();

        var result = new TestQueryBuilder(MockDb.Object, pk).Dynamic().ToList();

        await Assert.That(result).HasSingleItem();
        await Assert.That(items[0]).IsSameReferenceAs(result[0]);
        MockDb.Verify();
    }
}

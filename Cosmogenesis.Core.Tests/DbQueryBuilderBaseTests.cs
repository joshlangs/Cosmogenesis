using Microsoft.Azure.Cosmos;
using Moq;

namespace Cosmogenesis.Core.Tests;

public class DbQueryBuilderBaseTests
{
    class TestQueryBuilder : DbQueryBuilderBase
    {
        public TestQueryBuilder(DbBase dbBase, PartitionKey? partitionKey) : base(dbBase, partitionKey)
        {
        }
        public new IQueryable<T> BuildQueryByType<T>(string type) where T : DbDoc => base.BuildQueryByType<T>(type);
        public new IQueryable<T> BuildQueryByTypes<T>(string[] types) where T : DbDoc => base.BuildQueryByTypes<T>(types);
    }

    readonly Mock<DbBase> MockDb = new(MockBehavior.Strict);

    [Fact]
    [Trait("Type", "Unit")]
    public void Ctor_NullDb_Throws() => Assert.Throws<ArgumentNullException>(() => new TestQueryBuilder(null!, new PartitionKey("asdf")));

    [Fact]
    [Trait("Type", "Unit")]
    public void Ctor_NullPartitionKey_DoesNotThrow() => new TestQueryBuilder(MockDb.Object, null);

    [Fact]
    [Trait("Type", "Unit")]
    public void BuildQueryByType_Null_Throws() => Assert.Throws<ArgumentNullException>(() => new TestQueryBuilder(MockDb.Object, null).BuildQueryByType<TestDoc>(null!));

    [Fact]
    [Trait("Type", "Unit")]
    public void BuildQueryByTypes_Null_Throws() => Assert.Throws<ArgumentNullException>(() => new TestQueryBuilder(MockDb.Object, null).BuildQueryByTypes<TestDoc>(null!));

    [Fact]
    [Trait("Type", "Unit")]
    public void BuildQueryByType_Type_NoPartition_CreatesQuery()
    {
        var items = new List<TestDoc>
        {
            TestDoc.Instance,
            new TestDoc { Type = "asdf" },
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

        Assert.Single(result);
        Assert.Same(result[0], items[1]);
        MockDb.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void BuildQueryByTypes_Types_NoPartition_CreatesQuery()
    {
        var items = new List<TestDoc>
        {
            TestDoc.Instance,
            new TestDoc { Type = "asdf" },
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

        Assert.Single(result);
        Assert.Same(result[0], items[1]);
        MockDb.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void BuildQueryByType_Type_WithPartition_CreatesQuery()
    {
        var items = new List<TestDoc>
        {
            TestDoc.Instance,
            new TestDoc { Type = "asdf" },
            TestDoc.Instance
        };
        var q = items.AsQueryable().OrderBy(x => true);
        var pk = new PartitionKey("xxxx");
        MockDb
            .Setup(x => x.Container.GetItemLinqQueryable<TestDoc>(false, null, It.IsAny<QueryRequestOptions>(), It.IsAny<CosmosLinqSerializerOptions>()))
            .Returns((bool x, string y, QueryRequestOptions o, CosmosLinqSerializerOptions so) =>
            {
                Assert.Equal(pk, o.PartitionKey);
                return q;
            })
            .Verifiable();

        var result = new TestQueryBuilder(MockDb.Object, pk).BuildQueryByType<TestDoc>("asdf").ToList();

        Assert.Single(result);
        Assert.Same(result[0], items[1]);
        MockDb.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void BuildQueryByTypes_Types_WithPartition_CreatesQuery()
    {
        var items = new List<TestDoc>
        {
            TestDoc.Instance,
            new TestDoc { Type = "asdf" },
            TestDoc.Instance
        };
        var q = items.AsQueryable().OrderBy(x => true);
        var pk = new PartitionKey("xxxx");
        MockDb
            .Setup(x => x.Container.GetItemLinqQueryable<TestDoc>(false, null, It.IsAny<QueryRequestOptions>(), It.IsAny<CosmosLinqSerializerOptions>()))
            .Returns((bool x, string y, QueryRequestOptions o, CosmosLinqSerializerOptions so) =>
            {
                Assert.Equal(pk, o.PartitionKey);
                return q;
            })
            .Verifiable();

        var result = new TestQueryBuilder(MockDb.Object, pk).BuildQueryByTypes<TestDoc>(["asdf", "asdf2"]).ToList();

        Assert.Single(result);
        Assert.Same(result[0], items[1]);
        MockDb.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void Dynamic_NoPartition_CreatesQuery()
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

        Assert.Single(result);
        Assert.Same(result[0], items[0]);
        MockDb.Verify();
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void Dynamic_WithPartition_CreatesQuery()
    {
        var items = new List<IDictionary<string, object>>
        {
            new Dictionary<string, object>()
        };
        var q = items.AsQueryable().OrderBy(x => true);
        var pk = new PartitionKey("xxxx");
        MockDb
            .Setup(x => x.Container.GetItemLinqQueryable<IDictionary<string, object>>(false, null, It.IsAny<QueryRequestOptions>(), It.IsAny<CosmosLinqSerializerOptions>()))
            .Returns((bool x, string y, QueryRequestOptions o, CosmosLinqSerializerOptions so) =>
            {
                Assert.Equal(pk, o.PartitionKey);
                return q;
            })
            .Verifiable();

        var result = new TestQueryBuilder(MockDb.Object, pk).Dynamic().ToList();

        Assert.Single(result);
        Assert.Same(result[0], items[0]);
        MockDb.Verify();
    }
}

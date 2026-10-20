namespace Test.Cosmogenesis.Core;

public class IQueryableExtensionsTests
{
    [Test]
    public async Task ForceCast_SameList()
    {
        var vals = new List<DbDoc> { TestDoc.Instance };
        var vals2 = vals.AsQueryable().ForceCast<TestDoc>().ToList();
        await Assert.That(vals2).IsEquivalentTo(vals);
    }

    [Test]
    public async Task AsKnownDocument_SameList()
    {
        var vals = new List<TestDoc> { TestDoc.Instance };
        var vals2 = vals.AsQueryable().AsKnownDocument().ToList();
        await Assert.That(vals2).IsEquivalentTo(vals);
    }

    [Test]
    public async Task AsDynamic_SameList()
    {
        var vals = new List<object> { new Dictionary<string, object>() };
        var vals2 = vals.AsQueryable().AsDynamic().ToList();
        await Assert.That(vals2).IsEquivalentTo(vals);
    }
}

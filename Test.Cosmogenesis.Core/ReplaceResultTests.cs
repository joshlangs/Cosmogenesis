namespace Test.Cosmogenesis.Core;

public class ReplaceResultTests
{
    [Test]
    public async Task Ctor_Conflict_AllowedValues()
    {
        var allowed = new[]
        {
            DbConflictType.ETagChanged,
            DbConflictType.Missing
        };
        foreach (var conflict in EnumHelper<DbConflictType>.Values)
        {
            if (allowed.Contains(conflict))
            {
                await Assert.That(new ReplaceResult<TestDoc>(conflict).Conflict).IsEqualTo(conflict);
            }
            else
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => new ReplaceResult<TestDoc>(conflict));
            }
        }
    }

    [Test]
    public async Task ETagChanged_CorrectConflict()
    {
        Assert.Null(ReplaceResult<TestDoc>.ETagChanged.Document);
        await Assert.That(ReplaceResult<TestDoc>.ETagChanged.Conflict).IsEqualTo(DbConflictType.ETagChanged);
    }

    [Test]
    public async Task Missing_CorrectConflict()
    {
        Assert.Null(ReplaceResult<TestDoc>.Missing.Document);
        await Assert.That(ReplaceResult<TestDoc>.Missing.Conflict).IsEqualTo(DbConflictType.Missing);
    }

    [Test]
    public async Task Ctor_Doc_HasDoc()
    {
        Assert.Null(new ReplaceResult<TestDoc>(TestDoc.Instance).Conflict);
        await Assert.That(new ReplaceResult<TestDoc>(TestDoc.Instance).Document).IsSameReferenceAs(TestDoc.Instance);
    }
}

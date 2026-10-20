namespace Test.Cosmogenesis.Core;

public class DbChangeTests
{
    [Test]
    public async Task Ctor_Conflict_AllowedValues()
    {
        var allowed = new[]
        {
            DbConflictType.AlreadyExists,
            DbConflictType.ETagChanged,
            DbConflictType.Missing
        };
        foreach (var conflict in EnumHelper<DbConflictType>.Values)
        {
            if (allowed.Contains(conflict))
            {
                await Assert.That(new DbChange<TestDoc>(conflict).Conflict).IsEqualTo(conflict);
            }
            else
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => new DbChange<TestDoc>(conflict));
            }
        }
    }

    [Test]
    public async Task AlreadyExists_CorrectConflict()
    {
        Assert.Null(DbChange<TestDoc>.AlreadyExists.Document);
        await Assert.That(DbChange<TestDoc>.AlreadyExists.Conflict).IsEqualTo(DbConflictType.AlreadyExists);
    }

    [Test]
    public async Task ETagChanged_CorrectConflict()
    {
        Assert.Null(DbChange<TestDoc>.ETagChanged.Document);
        await Assert.That(DbChange<TestDoc>.ETagChanged.Conflict).IsEqualTo(DbConflictType.ETagChanged);
    }

    [Test]
    public async Task Missing_CorrectConflict()
    {
        Assert.Null(DbChange<TestDoc>.Missing.Document);
        await Assert.That(DbChange<TestDoc>.Missing.Conflict).IsEqualTo(DbConflictType.Missing);
    }

    [Test]
    public async Task Null_CorrectConflict()
    {
        Assert.Null(DbChange<TestDoc>.Null.Document);
        Assert.Null(DbChange<TestDoc>.Null.Conflict);
    }

    [Test]
    public async Task Ctor_Doc_HasDoc()
    {
        Assert.Null(new DbChange<TestDoc>(TestDoc.Instance).Conflict);
        await Assert.That(new DbChange<TestDoc>(TestDoc.Instance).Document).IsSameReferenceAs(TestDoc.Instance);
    }
}

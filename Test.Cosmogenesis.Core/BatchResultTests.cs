namespace Test.Cosmogenesis.Core;

public class BatchResultTests
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
                await Assert.That(new BatchResult(conflict).Conflict).IsEqualTo(conflict);
            }
            else
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => new BatchResult(conflict));
            }
        }
    }

    [Test]
    public async Task AlreadyExists_CorrectConflict()
    {
        Assert.Null(BatchResult.AlreadyExists.Documents);
        await Assert.That(BatchResult.AlreadyExists.Conflict).IsEqualTo(DbConflictType.AlreadyExists);
    }

    [Test]
    public async Task ETagChanged_CorrectConflict()
    {
        Assert.Null(BatchResult.ETagChanged.Documents);
        await Assert.That(BatchResult.ETagChanged.Conflict).IsEqualTo(DbConflictType.ETagChanged);
    }

    [Test]
    public async Task Missing_CorrectConflict()
    {
        Assert.Null(BatchResult.Missing.Documents);
        await Assert.That(BatchResult.Missing.Conflict).IsEqualTo(DbConflictType.Missing);
    }

    [Test]
    public async Task Ctor_Capacity_NoConflict()
    {
        Assert.Null(new BatchResult(0).Conflict);
        Assert.NotNull(new BatchResult(0).Documents);
    }
}

namespace Cosmogenesis.Core.Tests;

public class ReplaceResultTests
{
    [Fact]
    [Trait("Type", "Unit")]
    public void Ctor_Conflict_AllowedValues()
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
                Assert.Equal(conflict, new ReplaceResult<TestDoc>(conflict).Conflict);
            }
            else
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => new ReplaceResult<TestDoc>(conflict));
            }
        }
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void ETagChanged_CorrectConflict()
    {
        Assert.Null(ReplaceResult<TestDoc>.ETagChanged.Document);
        Assert.Equal(DbConflictType.ETagChanged, ReplaceResult<TestDoc>.ETagChanged.Conflict);
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void Missing_CorrectConflict()
    {
        Assert.Null(ReplaceResult<TestDoc>.Missing.Document);
        Assert.Equal(DbConflictType.Missing, ReplaceResult<TestDoc>.Missing.Conflict);
    }

    [Fact]
    [Trait("Type", "Unit")]
    public void Ctor_Doc_HasDoc()
    {
        Assert.Null(new ReplaceResult<TestDoc>(TestDoc.Instance).Conflict);
        Assert.Same(TestDoc.Instance, new ReplaceResult<TestDoc>(TestDoc.Instance).Document);
    }
}

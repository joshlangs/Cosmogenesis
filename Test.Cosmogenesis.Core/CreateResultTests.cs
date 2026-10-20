namespace Test.Cosmogenesis.Core;

public class CreateResultTests
{
    [Test]
    public async Task Ctor_Conflict_AllowedValues()
    {
        var allowed = new[]
        {
            DbConflictType.AlreadyExists
        };
        foreach (var conflict in EnumHelper<DbConflictType>.Values)
        {
            if (allowed.Contains(conflict))
            {
                await Assert.That(new CreateResult<TestDoc>(conflict).Conflict).IsEqualTo(conflict);
            }
            else
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => new CreateResult<TestDoc>(conflict));
            }
        }
    }

    [Test]
    public async Task AlreadyExists_CorrectConflict()
    {
        Assert.Null(CreateResult<TestDoc>.AlreadyExists.Document);
        await Assert.That(CreateResult<TestDoc>.AlreadyExists.Conflict).IsEqualTo(DbConflictType.AlreadyExists);
    }

    [Test]
    public async Task Ctor_Doc_HasDoc()
    {
        Assert.Null(new CreateResult<TestDoc>(TestDoc.Instance).Conflict);
        await Assert.That(new CreateResult<TestDoc>(TestDoc.Instance).Document).IsSameReferenceAs(TestDoc.Instance);
    }
}

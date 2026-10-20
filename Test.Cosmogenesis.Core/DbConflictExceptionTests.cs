namespace Test.Cosmogenesis.Core;

public class DbConflictExceptionTests
{
    [Test]
    public async Task Ctor_Conflict_FieldSet() => await Assert.That(new DbConflictException(DbConflictType.Missing).DbConflictType).IsEqualTo(DbConflictType.Missing);
}

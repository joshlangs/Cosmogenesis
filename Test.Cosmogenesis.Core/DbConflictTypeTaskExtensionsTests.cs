namespace Test.Cosmogenesis.Core;

public class DbConflictTypeTaskExtensionsTests
{
    [Test]
    public async Task ThrowOnConflict_Conflict_Throws() => await Assert.ThrowsAsync<DbConflictException>(() => Task.FromResult((DbConflictType?)DbConflictType.Missing).ThrowOnConflict());

    [Test]
    public async Task ThrowOnConflict_NoConflict_DoesNotThrow() => await Task.FromResult((DbConflictType?)null).ThrowOnConflict();
}

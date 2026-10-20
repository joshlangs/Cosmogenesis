namespace Test.Cosmogenesis.Core;

public class ReplaceResultTaskExtensionsTests
{
    [Test]
    public async Task ThrowOnConflict_Conflict_Throws() => await Assert.ThrowsAsync<DbConflictException>(() => Task.FromResult(ReplaceResult<TestDoc>.Missing).ThrowOnConflict());

    [Test]
    public async Task ThrowOnConflict_NoConflict_ReturnsResult() => await Assert.That(await Task.FromResult(new ReplaceResult<TestDoc>(TestDoc.Instance)).ThrowOnConflict()).IsSameReferenceAs(TestDoc.Instance);
}

namespace Test.Cosmogenesis.Core;

public class CreateResultTaskExtensionsTests
{
    [Test]
    public async Task ThrowOnConflict_Conflict_Throws() => await Assert.ThrowsAsync<DbConflictException>(() => Task.FromResult(CreateResult<TestDoc>.AlreadyExists).ThrowOnConflict());

    [Test]
    public async Task ThrowOnConflict_NoConflict_ReturnsResult() => await Assert.That(await Task.FromResult(new CreateResult<TestDoc>(TestDoc.Instance)).ThrowOnConflict()).IsSameReferenceAs(TestDoc.Instance);
}

namespace Test.Cosmogenesis.Core;

public class BatchResultTaskExtensionsTests
{
    [Test]
    public async Task ThrowOnConflict_Conflict_Throws() => await Assert.ThrowsAsync<DbConflictException>(() => Task.FromResult(BatchResult.AlreadyExists).ThrowOnConflict());

    [Test]
    public async Task ThrowOnConflict_NoConflict_ReturnsDocs() => Assert.NotNull(await Task.FromResult(new BatchResult(0)).ThrowOnConflict());
}

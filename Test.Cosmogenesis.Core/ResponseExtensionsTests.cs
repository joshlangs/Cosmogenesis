using System.Net;
using Microsoft.Azure.Cosmos;
using Moq;

namespace Test.Cosmogenesis.Core;

public class ResponseExtensionsTests
{
    readonly Mock<ResponseMessage> MockResponseMessage = new(MockBehavior.Strict);
    readonly Mock<TransactionalBatchResponse> MockTransactionalBatchResponse = new(MockBehavior.Strict);

    public ResponseExtensionsTests()
    {
        MockResponseMessage.Setup(x => x.StatusCode).Returns(HttpStatusCode.BadGateway).Verifiable();
        MockTransactionalBatchResponse.Setup(x => x.StatusCode).Returns(HttpStatusCode.BadGateway).Verifiable();
    }

    [Test]
    public async Task DbChangeFromErrorStatus_PreconditionFailed_ReturnsETagChanged() => await Assert.That(HttpStatusCode.PreconditionFailed.DbChangeFromErrorStatus<DbDoc>()).IsSameReferenceAs(DbChange<DbDoc>.ETagChanged);

    [Test]
    public async Task DbChangeFromErrorStatus_Conflict_ReturnsAlreadyExists() => await Assert.That(HttpStatusCode.Conflict.DbChangeFromErrorStatus<DbDoc>()).IsSameReferenceAs(DbChange<DbDoc>.AlreadyExists);

    [Test]
    public async Task DbChangeFromErrorStatus_NotFound_ReturnsMissing() => await Assert.That(HttpStatusCode.NotFound.DbChangeFromErrorStatus<DbDoc>()).IsSameReferenceAs(DbChange<DbDoc>.Missing);

    [Test]
    public async Task DbChangeFromErrorStatus_429_Throws() => Assert.Throws<DbOverloadedException>(() => ((HttpStatusCode)429).DbChangeFromErrorStatus<DbDoc>());

    [Test]
    public async Task DbChangeFromErrorStatusOther_Throws() => Assert.Throws<DbUnknownStatusCodeException>(() => HttpStatusCode.BadGateway.DbChangeFromErrorStatus<DbDoc>());

    [Test]
    public async Task CreateResultFromErrorStatus_Conflict_ReturnsAlreadyExists() => await Assert.That(HttpStatusCode.Conflict.CreateResultFromErrorStatus<DbDoc>()).IsSameReferenceAs(CreateResult<DbDoc>.AlreadyExists);

    [Test]
    public async Task CreateResultFromErrorStatus_429_Throws() => Assert.Throws<DbOverloadedException>(() => ((HttpStatusCode)429).CreateResultFromErrorStatus<DbDoc>());

    [Test]
    public async Task CreateResultFromErrorStatusOther_Throws() => Assert.Throws<DbUnknownStatusCodeException>(() => HttpStatusCode.BadGateway.CreateResultFromErrorStatus<DbDoc>());

    [Test]
    public async Task ReplaceResultFromErrorStatus_PreconditionFailed_ReturnsETagChanged() => await Assert.That(HttpStatusCode.PreconditionFailed.ReplaceResultFromErrorStatus<DbDoc>()).IsSameReferenceAs(ReplaceResult<DbDoc>.ETagChanged);

    [Test]
    public async Task ReplaceResultFromErrorStatus_NotFound_ReturnsMissing() => await Assert.That(HttpStatusCode.NotFound.ReplaceResultFromErrorStatus<DbDoc>()).IsSameReferenceAs(ReplaceResult<DbDoc>.Missing);

    [Test]
    public async Task ReplaceResultFromErrorStatus_429_Throws() => Assert.Throws<DbOverloadedException>(() => ((HttpStatusCode)429).ReplaceResultFromErrorStatus<DbDoc>());

    [Test]
    public async Task ReplaceResultFromErrorStatusOther_Throws() => Assert.Throws<DbUnknownStatusCodeException>(() => HttpStatusCode.BadGateway.ReplaceResultFromErrorStatus<DbDoc>());

    [Test]
    public async Task DeleteConflictTypeFromErrorStatus_PreconditionFailed_ReturnsETagChanged() => await Assert.That(HttpStatusCode.PreconditionFailed.DeleteConflictTypeFromErrorStatus()).IsEqualTo(DbConflictType.ETagChanged);

    [Test]
    public async Task DeleteConflictTypeFromErrorStatus_NotFound_ReturnsMissing() => await Assert.That(HttpStatusCode.NotFound.DeleteConflictTypeFromErrorStatus()).IsEqualTo(DbConflictType.Missing);

    [Test]
    public async Task DeleteConflictTypeFromErrorStatus_429_Throws() => Assert.Throws<DbOverloadedException>(() => ((HttpStatusCode)429).DeleteConflictTypeFromErrorStatus());

    [Test]
    public async Task DeleteConflictTypeFromErrorStatusOther_Throws() => Assert.Throws<DbUnknownStatusCodeException>(() => HttpStatusCode.BadGateway.DeleteConflictTypeFromErrorStatus());

    [Test]
    public async Task BatchResultFromErrorStatus_PreconditionFailed_ReturnsETagChanged() => await Assert.That(HttpStatusCode.PreconditionFailed.BatchResultFromErrorStatus()).IsEqualTo(BatchResult.ETagChanged);

    [Test]
    public async Task BatchResultFromErrorStatus_NotFound_ReturnsMissing() => await Assert.That(HttpStatusCode.NotFound.BatchResultFromErrorStatus()).IsEqualTo(BatchResult.Missing);

    [Test]
    public async Task BatchResultFromErrorStatus_Conflict_ReturnsAlreadyExists() => await Assert.That(HttpStatusCode.Conflict.BatchResultFromErrorStatus()).IsEqualTo(BatchResult.AlreadyExists);

    [Test]
    public async Task BatchResultFromErrorStatus_429_Throws() => Assert.Throws<DbOverloadedException>(() => ((HttpStatusCode)429).BatchResultFromErrorStatus());

    [Test]
    public async Task BatchResultFromErrorStatusOther_Throws() => Assert.Throws<DbUnknownStatusCodeException>(() => HttpStatusCode.BadGateway.BatchResultFromErrorStatus());

    [Test]
    public async Task DbChangeFromErrorStatus_ResponseMessage_Passthru()
    {
        try
        {
            MockResponseMessage.Object.DbChangeFromErrorStatus<DbDoc>();
            Assert.Fail("Should not happen");
        }
        catch (DbUnknownStatusCodeException e)
        {
            await Assert.That(e.StatusCode).IsEqualTo(HttpStatusCode.BadGateway);
        }
        MockResponseMessage.Verify();
    }

    [Test]
    public async Task CreateResultFromErrorStatus_ResponseMessage_Passthru()
    {
        try
        {
            MockResponseMessage.Object.CreateResultFromErrorStatus<DbDoc>();
            Assert.Fail("Should not happen");
        }
        catch (DbUnknownStatusCodeException e)
        {
            await Assert.That(e.StatusCode).IsEqualTo(HttpStatusCode.BadGateway);
        }
        MockResponseMessage.Verify();
    }

    [Test]
    public async Task ReplaceResultFromErrorStatus_ResponseMessage_Passthru()
    {
        try
        {
            MockResponseMessage.Object.ReplaceResultFromErrorStatus<DbDoc>();
            Assert.Fail("Should not happen");
        }
        catch (DbUnknownStatusCodeException e)
        {
            await Assert.That(e.StatusCode).IsEqualTo(HttpStatusCode.BadGateway);
        }
        MockResponseMessage.Verify();
    }

    [Test]
    public async Task DeleteConflictTypeFromErrorStatus_ResponseMessage_Passthru()
    {
        try
        {
            MockResponseMessage.Object.DeleteConflictTypeFromErrorStatus();
            Assert.Fail("Should not happen");
        }
        catch (DbUnknownStatusCodeException e)
        {
            await Assert.That(e.StatusCode).IsEqualTo(HttpStatusCode.BadGateway);
        }
        MockResponseMessage.Verify();
    }

    [Test]
    public async Task BatchResultFromErrorStatus_ResponseMessage_Passthru()
    {
        try
        {
            MockTransactionalBatchResponse.Object.BatchResultFromErrorStatus();
            Assert.Fail("Should not happen");
        }
        catch (DbUnknownStatusCodeException e)
        {
            await Assert.That(e.StatusCode).IsEqualTo(HttpStatusCode.BadGateway);
        }
        MockTransactionalBatchResponse.Verify();
    }

    [Test]
    public async Task ExceptionFromErrorStatus_ResponseMessage_Passthru()
    {
        var e = MockResponseMessage.Object.ExceptionFromErrorStatus() as DbUnknownStatusCodeException;
        await Assert.That(e!.StatusCode).IsEqualTo(HttpStatusCode.BadGateway);
        MockResponseMessage.Verify();
    }

    [Test]
    public async Task ExceptionFromErrorStatus_429_DbOverloadedException()
    {
        var e = ((HttpStatusCode)429).ExceptionFromErrorStatus();
        await Assert.That(e is DbOverloadedException).IsTrue();
    }

    [Test]
    public async Task ExceptionFromErrorStatus_Other_DbUnknownStatusCodeException()
    {
        var e = HttpStatusCode.BadGateway.ExceptionFromErrorStatus() as DbUnknownStatusCodeException;
        await Assert.That(e!.StatusCode).IsEqualTo(HttpStatusCode.BadGateway);
    }
}

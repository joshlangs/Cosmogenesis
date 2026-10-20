using System.Net;

namespace Test.Cosmogenesis.Core;

public class DbUnknownStatusCodeExceptionTests
{
    [Test]
    public async Task Ctor_StatusCode_SetsField() => await Assert.That(new DbUnknownStatusCodeException(HttpStatusCode.AlreadyReported).StatusCode).IsEqualTo(HttpStatusCode.AlreadyReported);
}

namespace Test.Cosmogenesis.Core;

public class ReadOrCreateResultTests
{
    [Test]
    public async Task Ctor_PropertiesSet()
    {
        var result = new ReadOrCreateResult<TestDoc>(TestDoc.Instance, true);
        await Assert.That(result.AlreadyExisted).IsTrue();
        await Assert.That(result.Document).IsSameReferenceAs(TestDoc.Instance);
    }
}

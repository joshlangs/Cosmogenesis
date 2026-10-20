namespace Test.Cosmogenesis.Core;

public class CreateOrReplaceResultTests
{
    [Test]
    public async Task Ctor_PropertiesSet()
    {
        var result = new CreateOrReplaceResult<TestDoc>(TestDoc.Instance, true);
        await Assert.That(result.AlreadyExisted).IsTrue();
        await Assert.That(result.Document).IsSameReferenceAs(TestDoc.Instance);
    }
}

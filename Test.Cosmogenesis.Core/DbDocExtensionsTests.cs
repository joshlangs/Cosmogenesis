namespace Test.Cosmogenesis.Core;

public class DbDocExtensionsTests
{
    [Test]
    public async Task GetApproxLastChangeDate_GivesCorrectDate()
    {
        var t = new TestDoc
        {
            _ts = RandomHelper.GetRandomPositiveInt32()
        };
        await Assert.That(t.GetApproxLastChangeDate()).IsEqualTo(DateTimeOffset.FromUnixTimeSeconds(t._ts).UtcDateTime);
    }
}

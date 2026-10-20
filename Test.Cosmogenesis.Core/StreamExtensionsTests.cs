namespace Test.Cosmogenesis.Core;

public class StreamExtensionsTests
{
    [Test]
    public async Task ToSpan_GetsSpan()
    {
        using var ms = new MemoryStream(RandomHelper.GetRandomBytes(123));
        await Assert.That(ms.ToSpan().Length).IsEqualTo((int)ms.Length);
        await Assert.That(ms.ToSpan().SequenceEqual(ms.ToArray())).IsTrue();
    }
}

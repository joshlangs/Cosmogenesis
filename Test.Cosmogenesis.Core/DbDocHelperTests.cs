namespace Test.Cosmogenesis.Core;

public class DbDocHelperTests
{
    [Test]
    public async Task GetValidId_Null_Throws() => Assert.Throws<ArgumentNullException>(() => DbDocHelper.GetValidId(null!));

    [Test]
    public async Task GetValidId_TooLong_Throws() => Assert.Throws<ArgumentOutOfRangeException>(() => DbDocHelper.GetValidId(new string('a', DbDocHelper.MaxIdBytes + 1)));

    [Test]
    public async Task GetValidId_MaxLength_Returns() => await Assert.That(DbDocHelper.GetValidId(new string('a', DbDocHelper.MaxIdBytes))).IsEqualTo(new string('a', DbDocHelper.MaxIdBytes));

    [Test]
    public async Task GetValidId_Empty_ReturnsEmpty() => await Assert.That(DbDocHelper.GetValidId("")).IsEqualTo("");

    [Test]
    public async Task GetValidId_InvalidChars_GetReplaced() => await Assert.That(DbDocHelper.GetValidId("a/\\?#b")).IsEqualTo("a____b");
}

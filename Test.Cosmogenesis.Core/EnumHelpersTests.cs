namespace Test.Cosmogenesis.Core;

public class EnumHelpersTests
{
    enum TestEnum
    {
        Apples,
        Bananas = 4,
#pragma warning disable CA1069 // Enums values should not be duplicated
        Oranges = 4,
#pragma warning restore CA1069 // Enums values should not be duplicated
        OtherFruit = 10
    }

    [Test]
    public async Task Values_TestEnum_ReturnsAllEnums() => await Assert
        .That(EnumHelper<TestEnum>.Values.OrderBy(x => x.ToString()))
        .IsEquivalentTo(new[] { TestEnum.Apples, TestEnum.Bananas, TestEnum.Oranges, TestEnum.OtherFruit }.OrderBy(x => x.ToString()));
}

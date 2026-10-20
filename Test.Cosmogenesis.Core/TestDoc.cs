namespace Test.Cosmogenesis.Core;

class TestDoc : DbDoc
{
    public static readonly TestDoc Instance = new();

    protected override bool ValidateState() => true;
}

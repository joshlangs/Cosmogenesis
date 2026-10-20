namespace Test.Cosmogenesis.Core;

public class DbDocTests
{
    readonly DateTime Now = DateTime.UtcNow;

    [Test]
    public async Task SetTypeTwice_NoChange_DoesNotThrow()
    {
        var t = new TestDoc
        {
            Type = "Asdf",
            _etag = "asdf"
        };
        t.Type = "Asdf";
    }

    [Test]
    public async Task SetTypeTwice_Change_Throws()
    {
        var t = new TestDoc
        {
            Type = "Asdf",
            _etag = "asdf"
        };
        Assert.Throws<Exception>(() => t.Type = "Explode");
    }

    [Test]
    public async Task SetCreationDateTwice_NoChange_DoesNotThrow()
    {
        var t = new TestDoc
        {
            CreationDate = Now,
            _etag = "asdf"
        };
        t.CreationDate = Now;
    }

    [Test]
    public async Task SetCreationDateTwice_Change_Throws()
    {
        var t = new TestDoc
        {
            CreationDate = Now,
            _etag = "asdf"
        };
        Assert.Throws<Exception>(() => t.CreationDate = Now.AddSeconds(1));
    }
}

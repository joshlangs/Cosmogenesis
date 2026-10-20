using System.Text;
using System.Text.Json;

namespace Test.Cosmogenesis.Core;

public class DbDocConverterBaseTests : DbDocConverterBase
{
    readonly JsonSerializerOptions JsonSerializerOptions = new();

    [Test]
    public async Task Write_Throws() => Assert.Throws<NotImplementedException>(() => Write(null!, null!, null!));

    [Test]
    public async Task Read_NoType_Throws()
    {
        var json = "{}";
        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));
        try
        {
            Read(ref reader, typeof(object), JsonSerializerOptions);
            Assert.Fail("Should not happen");
        }
        catch (NotSupportedException)
        {
        }
    }

    [Test]
    public async Task Read_WithType_CallsDeserializeByType()
    {
        var json = @"{""Type"" : ""test""}";
        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));

        var result = Read(ref reader, typeof(object), JsonSerializerOptions);

        await Assert.That(result).IsSameReferenceAs(TestDoc.Instance);
        await Assert.That(Type).IsEqualTo("test");
        await Assert.That(JsonSerializerOptions).IsSameReferenceAs(Options);
    }

    new string? Type;
    JsonSerializerOptions? Options;
    protected override DbDoc DeserializeByType(ref Utf8JsonReader reader, string? type, JsonSerializerOptions options)
    {
        Type = type;
        Options = options;
        return TestDoc.Instance;
    }
}

using System.Text;
using System.Text.Json;

namespace Cosmogenesis.Core.Tests;

public class DbDocConverterBaseTests: DbDocConverterBase
{
    readonly JsonSerializerOptions JsonSerializerOptions = new();

    [Fact]
    [Trait("Type", "Unit")]
    public void Write_Throws() => Assert.Throws<NotImplementedException>(() => Write(null!, null!, null!));

    [Fact]
    [Trait("Type", "Unit")]
    public void Read_NoType_Throws()
    {
        var json = "{}";
        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));
        try
        {
            Read(ref reader, typeof(object), JsonSerializerOptions);
            Assert.False(true);
        }
        catch (NotSupportedException)
        {
        }
    }
    
    [Fact]
    [Trait("Type", "Unit")]
    public void Read_WithType_CallsDeserializeByType()
    {
        var json = @"{""Type"" : ""test""}";
        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));
        
        var result = Read(ref reader, typeof(object), JsonSerializerOptions);

        Assert.Same(TestDoc.Instance, result);
        Assert.Equal("test", Type);
        Assert.Same(Options, JsonSerializerOptions);
    }

    new string? Type;
    JsonSerializerOptions? Options;
    protected override DbDoc DeserializeByType(ReadOnlySpan<byte> data, string? type, JsonSerializerOptions options)
    {
        Type = type;
        Options = options;
        return TestDoc.Instance;
    }
}

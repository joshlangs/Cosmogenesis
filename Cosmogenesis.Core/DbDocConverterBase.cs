using System.Text.Json;
using System.Text.Json.Serialization;

namespace Cosmogenesis.Core;

public abstract class DbDocConverterBase : JsonConverter<DbDoc>
{
    public sealed override DbDoc Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var start = reader.TokenStartIndex;
        using var doc = JsonDocument.ParseValue(ref reader);
        if (!doc.RootElement.TryGetProperty(nameof(DbDoc.Type), out var value))
        {
            throw new NotSupportedException($"We don't understand how to deserialize this message");
        }
        var end = reader.BytesConsumed;        
        using var ms = new MemoryStream((int)(end - start + 1));
        using var writer = new Utf8JsonWriter(ms);
        doc.WriteTo(writer);
        writer.Flush();
        var type = value.GetString();
        return DeserializeByType(ms.GetDataSpan(), type, options) ?? throw new NotSupportedException($"We cannot deserialize {type} into null");
    }

    protected abstract DbDoc? DeserializeByType(ReadOnlySpan<byte> data, string? type, JsonSerializerOptions options);

    public override sealed void Write(Utf8JsonWriter writer, DbDoc value, JsonSerializerOptions options) => throw new NotImplementedException();
}

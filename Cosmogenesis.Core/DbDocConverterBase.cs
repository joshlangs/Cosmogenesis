using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Cosmogenesis.Core;

public abstract class DbDocConverterBase : JsonConverter<DbDoc>
{
    static readonly byte[] TypeBytes = Encoding.UTF8.GetBytes(nameof(DbDoc.Type));
    public sealed override DbDoc Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var clone = reader;
        if (clone.Read() && clone.TokenType == JsonTokenType.StartObject)
        {
            while (clone.Read() && clone.TokenType == JsonTokenType.PropertyName)
            {
                if (clone.ValueTextEquals(TypeBytes))
                {
                    clone.Read();
                    var type = clone.GetString();
                    return DeserializeByType(ref reader, type, options) ?? throw new NotSupportedException($"We cannot deserialize {type} into null");
                }
                clone.Skip();
            }
        }
        throw new NotSupportedException($"We don't understand how to deserialize this message");
    }

    protected abstract DbDoc? DeserializeByType(ref Utf8JsonReader reader, string? type, JsonSerializerOptions options);

    public override sealed void Write(Utf8JsonWriter writer, DbDoc value, JsonSerializerOptions options) => throw new NotImplementedException();
}

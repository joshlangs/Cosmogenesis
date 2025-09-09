using System.Diagnostics.CodeAnalysis;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using Epoche.Shared.Json;
using Microsoft.Azure.Cosmos;

namespace Cosmogenesis.Core;

public abstract class DbSerializerBase : CosmosSerializer
{
    static JsonSerializerOptions CreateJsonSerializerOptions(JsonIgnoreCondition defaultIgnoreCondition) => new JsonSerializerOptions
    {
        DefaultIgnoreCondition = defaultIgnoreCondition,
        Converters =
        {
            ByteArrayConverter.Instance,
            Int64Converter.Instance,
            UInt64Converter.Instance,
            IsoDateTimeConverter.Instance,
            DecimalConverter.Instance,
            BigIntegerConverter.Instance,
            new JsonStringEnumConverter(),
            BigFractionConverter.Instance,
            IPAddressConverter.Instance,
            DateOnlyConverter.Instance,
            Int128Converter.Instance,
            UInt128Converter.Instance
        },
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        PreferredObjectCreationHandling = JsonObjectCreationHandling.Populate
    };

    protected virtual JsonSerializerOptions SerializeOptions { get; } = CreateJsonSerializerOptions(JsonIgnoreCondition.Never);
    protected virtual JsonSerializerOptions DeserializeOptions { get; } = CreateJsonSerializerOptions(JsonIgnoreCondition.WhenWritingNull);

    public override Stream ToStream<T>(T input)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(
            value: input,
            options: SerializeOptions);
        return new MemoryStream(bytes);
    }

    protected static class DeserializeDbDocCache<T>
    {
        public static readonly bool IsDbDoc = typeof(T) == typeof(DbDoc) || typeof(T).IsSubclassOf(typeof(DbDoc));
    }

    [return: MaybeNull]
    public override T FromStream<T>(Stream stream)
    {
        if (stream is null)
        {
            throw new ArgumentNullException(nameof(stream));
        }

        var item = FromStream<T>(stream.ToSpan());
        stream.Dispose();
        return item;
    }

    [return: MaybeNull]
    public virtual T FromStream<T>(ReadOnlySpan<byte> data)
    {
        var reader = new Utf8JsonReader(data);
        if (DeserializeDbDocCache<T>.IsDbDoc)
        {
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.PropertyName)
                {
                    if (reader.GetString() == nameof(DbDoc.Type))
                    {
                        if (!reader.Read())
                        {
                            break;
                        }

                        var type = reader.GetString();
                        return (T)(object?)DeserializeByType(data, type)!;
                    }
                    reader.Skip();
                }
            }
            throw new NotSupportedException($"We don't understand how to deserialize this message");
        }
        return JsonSerializer.Deserialize<T>(ref reader, DeserializeOptions);
    }

    public T Clone<T>(T original)
    {
        if (original is null)
        {
            throw new ArgumentNullException(nameof(original));
        }
        return FromStream<T>(JsonSerializer.SerializeToUtf8Bytes(
            value: original,
            options: SerializeOptions))!;
    }

    protected abstract DbDoc? DeserializeByType(ReadOnlySpan<byte> data, string? type);

    public virtual List<T> DeserializeDocumentList<T>(Stream stream)
    {
        if (stream is null)
        {
            throw new ArgumentNullException(nameof(stream));
        }

        var data = stream.ToSpan();
        var reader = new Utf8JsonReader(data);
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.PropertyName)
            {
                if (reader.GetString() == "Documents")
                {
                    reader.Read();
                    if (reader.TokenType != JsonTokenType.StartArray)
                    {
                        break;
                    }

                    var items = new List<T>();
                    while (true)
                    {
                        reader.Read();
                        if (reader.TokenType == JsonTokenType.EndArray)
                        {
                            return items;
                        }

                        var start = (int)reader.TokenStartIndex;
                        reader.Skip();
                        var end = (int)reader.BytesConsumed;
                        items.Add(FromStream<T>(data[start..end])!);
                    }
                }
                reader.Skip();
            }
        }
        throw new NotSupportedException($"We don't understand how to extract results from the query");
    }
}

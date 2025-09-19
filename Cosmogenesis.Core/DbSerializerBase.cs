﻿using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using Epoche.Shared.Json;
using Microsoft.Azure.Cosmos;

namespace Cosmogenesis.Core;

public abstract class DbSerializerBase : CosmosSerializer
{
    static readonly byte[] TypeBytes = Encoding.UTF8.GetBytes(nameof(DbDoc.Type));
    static readonly byte[] DocumentsBytes = Encoding.UTF8.GetBytes("Documents");
    static JsonSerializerOptions CreateJsonSerializerOptions(JsonIgnoreCondition defaultIgnoreCondition) => new()
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
        ArgumentNullException.ThrowIfNull(stream);
        using (stream)
        {
            return FromStream<T>(stream.ToSpan());
        }
    }

    [return: MaybeNull]
    public virtual T FromStream<T>(ReadOnlySpan<byte> data)
    {
        var reader = new Utf8JsonReader(data);
        if (DeserializeDbDocCache<T>.IsDbDoc)
        {
            if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
            {
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    if (reader.ValueTextEquals(TypeBytes))
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
        ArgumentNullException.ThrowIfNull(original);
        return FromStream<T>(JsonSerializer.SerializeToUtf8Bytes(
            value: original,
            options: SerializeOptions))!;
    }

    protected abstract DbDoc? DeserializeByType(ReadOnlySpan<byte> data, string? type);

    public virtual List<T> DeserializeDocumentList<T>(Stream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);

        var data = stream.ToSpan();
        var reader = new Utf8JsonReader(data);
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.PropertyName)
            {
                if (reader.ValueTextEquals(DocumentsBytes))
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

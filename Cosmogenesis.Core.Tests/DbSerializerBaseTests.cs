using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Cosmogenesis.Core.Tests;

public class DbSerializerBaseTests
{
    static string StreamToString(Stream stream) => Encoding.UTF8.GetString(stream.ToSpan().ToArray());


    class TestClass
    {
        public List<string> Stuff { get; } = [];
    }


    public class NullSerializer : DbSerializerBase
    {
        protected override DbDoc DeserializeByType(ReadOnlySpan<byte> data, string? type) => throw new NotImplementedException();

        [Fact]
        [Trait("Type", "Unit")]
        public void DeserializeDbDocCache_Object_NotDbDoc() => Assert.False(DeserializeDbDocCache<object>.IsDbDoc);

        [Fact]
        [Trait("Type", "Unit")]
        public void DeserializeDbDocCache_Int_NotDbDoc() => Assert.False(DeserializeDbDocCache<int>.IsDbDoc);

        [Fact]
        [Trait("Type", "Unit")]
        public void DeserializeDbDocCache_DbDoc_IsDbDoc() => Assert.True(DeserializeDbDocCache<DbDoc>.IsDbDoc);

        [Fact]
        [Trait("Type", "Unit")]
        public void DeserializeDbDocCache_TestDoc_IsDbDoc() => Assert.True(DeserializeDbDocCache<TestDoc>.IsDbDoc);

        [Fact]
        [Trait("Type", "Unit")]
        public void SerializeOptions_DoesNotIgnoreNullValues() => Assert.Equal(JsonIgnoreCondition.Never, SerializeOptions.DefaultIgnoreCondition);

        [Fact]
        [Trait("Type", "Unit")]
        public void DeserializeOptions_IgnoresNullValues() => Assert.Equal(JsonIgnoreCondition.WhenWritingNull, DeserializeOptions.DefaultIgnoreCondition);

        [Fact]
        [Trait("Type", "Unit")]
        public void ToStream_ReturnsMemoryStream() => Assert.IsType<MemoryStream>(ToStream(TestDoc.Instance));

        [Fact]
        [Trait("Type", "Unit")]
        public void ToStream_Doc_HasNoTtl() => Assert.DoesNotContain("ttl", Encoding.UTF8.GetString(((MemoryStream)ToStream(TestDoc.Instance)).ToSpan()));

        [Fact]
        [Trait("Type", "Unit")]
        public void ToFromStream_Roundtrip_4() => Assert.Equal(4, FromStream<int>(ToStream(4)));

        [Fact]
        [Trait("Type", "Unit")]
        public void ToFromStream_Roundtrip_String() => Assert.Equal("test", FromStream<string>(ToStream("test")));

        [Fact]
        [Trait("Type", "Unit")]
        public void ToFromStream_Roundtrip_Null() => Assert.Null(FromStream<string>(ToStream((string?)null)));

        [Fact]
        [Trait("Type", "Unit")]
        public void ToStream_Int64_TestCases()
        {
            for (var x = 0; x < 100; ++x)
            {
                var value = RandomHelper.GetRandomInt64();
                var result = $@"""{value}""";
                Assert.Equal(result, StreamToString(ToStream(value)));
            }
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void ToStream_ByteArray_TestCases()
        {
            for (var x = 0; x < 100; ++x)
            {
                var value = RandomHelper.GetRandomBytes(x);
                var result = $@"""{value.ToLowerHex()}""";
                Assert.Equal(result, StreamToString(ToStream(value)));
            }
        }

        enum TestEnum { EnumValue }

        [Fact]
        [Trait("Type", "Unit")]
        public void ToStream_EnumValue_String()
        {
            var stream = ToStream(TestEnum.EnumValue);

            Assert.Equal($@"""EnumValue""", StreamToString(stream));
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void FromStream_String_EnumValue()
        {
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes($@"""EnumValue"""));
            var actual = FromStream<TestEnum>(stream);

            Assert.Equal(TestEnum.EnumValue, actual);
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void FromStream_Stream_DisposesStream()
        {
            using var ms = new MemoryStream(Encoding.UTF8.GetBytes("null"));
            FromStream<object>(ms);
            Assert.False(ms.CanRead);
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void FromStream_NullStream_Throws() => Assert.Throws<ArgumentNullException>(() => FromStream<int>((Stream)null!));

        [Fact]
        [Trait("Type", "Unit")]
        public void DeserializeDocumentList_Null_Throws() => Assert.Throws<ArgumentNullException>(() => DeserializeDocumentList<int>(null!));

        [Fact]
        [Trait("Type", "Unit")]
        public void DeserializeDocumentList_NoDocuments_Throws()
        {
            var s = ToStream(new { asd = "def" });
            Assert.Throws<NotSupportedException>(() => DeserializeDocumentList<int>(s));
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void DeserializeDocumentList_DocumentsNull_Throws()
        {
            var s = ToStream(new { Documents = (object?)null });
            Assert.Throws<NotSupportedException>(() => DeserializeDocumentList<int>(s));
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void DeserializeDocumentList_DocumentsObject_Throws()
        {
            var s = ToStream(new { Documents = new { x = 4 } });
            Assert.Throws<NotSupportedException>(() => DeserializeDocumentList<int>(s));
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void DeserializeDocumentList_DocumentsString_Throws()
        {
            var s = ToStream(new { Documents = "hi" });
            Assert.Throws<NotSupportedException>(() => DeserializeDocumentList<int>(s));
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void DeserializeDocumentList_DocumentsWrongCase_Throws()
        {
            var s = ToStream(new { documents = Array.Empty<object>() });
            Assert.Throws<NotSupportedException>(() => DeserializeDocumentList<int>(s));
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void DeserializeDocumentList_DocumentsEmptyArray_ReturnsEmpty()
        {
            var s = ToStream(new { Documents = Array.Empty<int>() });
            Assert.Empty(DeserializeDocumentList<int>(s));
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void DeserializeDocumentList_DocumentsArray_ReturnsSameValues()
        {
            var vals = new[] { 4, 5, 6 };
            var s = ToStream(new { Documents = vals });
            Assert.Equal(vals, DeserializeDocumentList<int>(s));
        }

        [Fact]
        public void Deserialize_ReadOnlyCollection_Populates()
        {
            var obj = JsonSerializer.Deserialize<TestClass>("{\"Stuff\":[\"a\",\"bb\"]}", DeserializeOptions);
            Assert.Equal(obj!.Stuff.ToArray().AsSpan(), ["a", "bb"]);
        }
    }

    public class FromStreamSpanSerializer : DbSerializerBase
    {
        protected override DbDoc DeserializeByType(ReadOnlySpan<byte> data, string? type) => throw new NotImplementedException();

        byte[]? Data;
        public override T FromStream<T>(ReadOnlySpan<byte> data)
        {
            Data = data.ToArray();
            return default!;
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void FromStream_Stream_CallsFromStream_Span()
        {
            var bytes = Encoding.UTF8.GetBytes("null");
            using var ms = new MemoryStream(bytes);
            FromStream<int>(ms);
            Assert.Equal(bytes, Data);
        }
    }

    public class FromStreamByTypeSerializer : DbSerializerBase
    {
        bool Called;
        byte[]? Data;
        string? Type;
        protected override DbDoc DeserializeByType(ReadOnlySpan<byte> data, string? type)
        {
            Called = true;
            Data = data.ToArray();
            Type = type;
            return TestDoc.Instance;
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void FromStream_NotDbDoc_DoesntCallByType()
        {
            FromStream<string>(ToStream("asdf"));
            Assert.False(Called);
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void FromStream_DbDoc_NoType_Throws()
        {
            Assert.Throws<NotSupportedException>(() => FromStream<DbDoc>(ToStream(new { abc = "def" })));
            Assert.False(Called);
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void FromStream_DbDoc_TypeNull_CallsByType()
        {
            FromStream<DbDoc>(ToStream(TestDoc.Instance));
            Assert.True(Called);
            Assert.Null(Type);
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void FromStream_DbDoc_TypeSet_CallsByType()
        {
            FromStream<DbDoc>(ToStream(new TestDoc { Type = "asdf" }));
            Assert.True(Called);
            Assert.Equal("asdf", Type);
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void FromStream_TestDoc_DoesNotThrow()
        {
            FromStream<TestDoc>(ToStream(TestDoc.Instance));
            Assert.True(Called);
        }

        class WrongType : DbDoc { }

        [Fact]
        [Trait("Type", "Unit")]
        public void FromStream_WrongDbDocType_Throws()
        {
            Assert.Throws<InvalidCastException>(() => FromStream<WrongType>(ToStream(TestDoc.Instance)));
            Assert.True(Called);
        }

        [Fact]
        [Trait("Type", "Unit")]
        public void FromStream_ByType_SameArray()
        {
            var s = ToStream(new TestDoc { Type = "asdf" });
            FromStream<DbDoc>(s);
            Assert.True(Called);
            Assert.Equal(s.ToSpan().ToArray(), Data);
        }
    }
}

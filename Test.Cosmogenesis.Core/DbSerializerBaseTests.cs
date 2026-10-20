using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Test.Cosmogenesis.Core;

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

        [Test]
        public async Task DeserializeDbDocCache_Object_NotDbDoc() => await Assert.That(DeserializeDbDocCache<object>.IsDbDoc).IsFalse();

        [Test]
        public async Task DeserializeDbDocCache_Int_NotDbDoc() => await Assert.That(DeserializeDbDocCache<int>.IsDbDoc).IsFalse();

        [Test]
        public async Task DeserializeDbDocCache_DbDoc_IsDbDoc() => await Assert.That(DeserializeDbDocCache<DbDoc>.IsDbDoc).IsTrue();

        [Test]
        public async Task DeserializeDbDocCache_TestDoc_IsDbDoc() => await Assert.That(DeserializeDbDocCache<TestDoc>.IsDbDoc).IsTrue();

        [Test]
        public async Task SerializeOptions_DoesNotIgnoreNullValues() => await Assert.That(SerializeOptions.DefaultIgnoreCondition).IsEqualTo(JsonIgnoreCondition.Never);

        [Test]
        public async Task DeserializeOptions_IgnoresNullValues() => await Assert.That(DeserializeOptions.DefaultIgnoreCondition).IsEqualTo(JsonIgnoreCondition.WhenWritingNull);

        [Test]
        public async Task ToStream_ReturnsMemoryStream() => await Assert.That(ToStream(TestDoc.Instance)).IsOfType(typeof(MemoryStream));

        [Test]
        public async Task ToStream_Doc_HasNoTtl() => await Assert.That(Encoding.UTF8.GetString(((MemoryStream)ToStream(TestDoc.Instance)).ToSpan())).DoesNotContain("ttl");

        [Test]
        public async Task ToFromStream_Roundtrip_4() => await Assert.That(FromStream<int>(ToStream(4))).IsEqualTo(4);

        [Test]
        public async Task ToFromStream_Roundtrip_String() => await Assert.That(FromStream<string>(ToStream("test"))).IsEqualTo("test");

        [Test]
        public async Task ToFromStream_Roundtrip_Null() => Assert.Null(FromStream<string>(ToStream((string?)null)));

        [Test]
        public async Task ToStream_Int64_TestCases()
        {
            for (var x = 0; x < 100; ++x)
            {
                var value = RandomHelper.GetRandomInt64();
                var result = $@"""{value}""";
                await Assert.That(StreamToString(ToStream(value))).IsEqualTo(result);
            }
        }

        [Test]
        public async Task ToStream_ByteArray_TestCases()
        {
            for (var x = 0; x < 100; ++x)
            {
                var value = RandomHelper.GetRandomBytes(x);
                var result = $@"""{value.ToLowerHex()}""";
                await Assert.That(StreamToString(ToStream(value))).IsEqualTo(result);
            }
        }

        enum TestEnum { EnumValue }

        [Test]
        public async Task ToStream_EnumValue_String()
        {
            var stream = ToStream(TestEnum.EnumValue);

            await Assert.That(StreamToString(stream)).IsEqualTo($@"""EnumValue""");
        }

        [Test]
        public async Task FromStream_String_EnumValue()
        {
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes($@"""EnumValue"""));
            var actual = FromStream<TestEnum>(stream);

            await Assert.That(actual).IsEqualTo(TestEnum.EnumValue);
        }

        [Test]
        public async Task FromStream_Stream_DisposesStream()
        {
            using var ms = new MemoryStream(Encoding.UTF8.GetBytes("null"));
            FromStream<object>(ms);
            await Assert.That(ms.CanRead).IsFalse();
        }

        [Test]
        public async Task FromStream_NullStream_Throws() => Assert.Throws<ArgumentNullException>(() => FromStream<int>((Stream)null!));

        [Test]
        public async Task DeserializeDocumentList_Null_Throws() => Assert.Throws<ArgumentNullException>(() => DeserializeDocumentList<int>(null!));

        [Test]
        public async Task DeserializeDocumentList_NoDocuments_Throws()
        {
            var s = ToStream(new { asd = "def" });
            Assert.Throws<NotSupportedException>(() => DeserializeDocumentList<int>(s));
        }

        [Test]
        public async Task DeserializeDocumentList_DocumentsNull_Throws()
        {
            var s = ToStream(new { Documents = (object?)null });
            Assert.Throws<NotSupportedException>(() => DeserializeDocumentList<int>(s));
        }

        [Test]
        public async Task DeserializeDocumentList_DocumentsObject_Throws()
        {
            var s = ToStream(new { Documents = new { x = 4 } });
            Assert.Throws<NotSupportedException>(() => DeserializeDocumentList<int>(s));
        }

        [Test]
        public async Task DeserializeDocumentList_DocumentsString_Throws()
        {
            var s = ToStream(new { Documents = "hi" });
            Assert.Throws<NotSupportedException>(() => DeserializeDocumentList<int>(s));
        }

        [Test]
        public async Task DeserializeDocumentList_DocumentsWrongCase_Throws()
        {
            var s = ToStream(new { documents = Array.Empty<object>() });
            Assert.Throws<NotSupportedException>(() => DeserializeDocumentList<int>(s));
        }

        [Test]
        public async Task DeserializeDocumentList_DocumentsEmptyArray_ReturnsEmpty()
        {
            var s = ToStream(new { Documents = Array.Empty<int>() });
            await Assert.That(DeserializeDocumentList<int>(s)).IsEmpty();
        }

        [Test]
        public async Task DeserializeDocumentList_DocumentsArray_ReturnsSameValues()
        {
            var vals = new[] { 4, 5, 6 };
            var s = ToStream(new { Documents = vals });
            await Assert.That(DeserializeDocumentList<int>(s)).IsEquivalentTo(vals);
        }

        [Test]
        public async Task Deserialize_ReadOnlyCollection_Populates()
        {
            var obj = JsonSerializer.Deserialize<TestClass>("{\"Stuff\":[\"a\",\"bb\"]}", DeserializeOptions);
            await Assert.That(["a", "bb"]).IsEquivalentTo(obj!.Stuff.ToArray().AsSpan().ToArray());
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

        [Test]
        public async Task FromStream_Stream_CallsFromStream_Span()
        {
            var bytes = Encoding.UTF8.GetBytes("null");
            using var ms = new MemoryStream(bytes);
            FromStream<int>(ms);
            await Assert.That(Data).IsEquivalentTo(bytes);
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

        [Test]
        public async Task FromStream_NotDbDoc_DoesntCallByType()
        {
            FromStream<string>(ToStream("asdf"));
            await Assert.That(Called).IsFalse();
        }

        [Test]
        public async Task FromStream_DbDoc_NoType_Throws()
        {
            Assert.Throws<NotSupportedException>(() => FromStream<DbDoc>(ToStream(new { abc = "def" })));
            await Assert.That(Called).IsFalse();
        }

        [Test]
        public async Task FromStream_DbDoc_TypeNull_CallsByType()
        {
            FromStream<DbDoc>(ToStream(TestDoc.Instance));
            await Assert.That(Called).IsTrue();
            Assert.Null(Type);
        }

        [Test]
        public async Task FromStream_DbDoc_TypeSet_CallsByType()
        {
            FromStream<DbDoc>(ToStream(new TestDoc { Type = "asdf" }));
            await Assert.That(Called).IsTrue();
            await Assert.That(Type).IsEqualTo("asdf");
        }

        [Test]
        public async Task FromStream_TestDoc_DoesNotThrow()
        {
            FromStream<TestDoc>(ToStream(TestDoc.Instance));
            await Assert.That(Called).IsTrue();
        }

        class WrongType : DbDoc { }

        [Test]
        public async Task FromStream_WrongDbDocType_Throws()
        {
            Assert.Throws<InvalidCastException>(() => FromStream<WrongType>(ToStream(TestDoc.Instance)));
            await Assert.That(Called).IsTrue();
        }

        [Test]
        public async Task FromStream_ByType_SameArray()
        {
            var s = ToStream(new TestDoc { Type = "asdf" });
            FromStream<DbDoc>(s);
            await Assert.That(Called).IsTrue();
            await Assert.That(Data).IsEquivalentTo(s.ToSpan().ToArray());
        }
    }
}

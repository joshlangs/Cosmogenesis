namespace Cosmogenesis.Core.Attributes;

[AttributeUsage(AttributeTargets.Class)]
/// <summary>
/// Specifies a document is transient (can be deleted after creation).
/// </summary>
public sealed class TransientAttribute : Attribute
{
    public readonly bool AutoExpires;
    public readonly int? DefaultTtl;

    public TransientAttribute(int defaultTtl) : this(true)
    {
        DefaultTtl = defaultTtl;
    }

    public TransientAttribute(bool autoExpires)
    {
        AutoExpires = autoExpires;
    }

    public TransientAttribute() { }
}

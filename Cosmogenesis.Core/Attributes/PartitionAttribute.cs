namespace Cosmogenesis.Core.Attributes;

[AttributeUsage(AttributeTargets.Class)]
/// <summary>
/// Specifies in which partition a document belongs.
/// </summary>
public sealed class PartitionAttribute(string name) : Attribute
{
    public readonly string Name = name;
}

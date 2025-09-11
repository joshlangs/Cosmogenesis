namespace Cosmogenesis.Core.Attributes;

[AttributeUsage(AttributeTargets.Class)]
/// <summary>
/// Specifies the .Type field assigned to a document.
/// </summary>
public sealed class DocTypeAttribute(string name) : Attribute
{
    public readonly string Name = name;
}

namespace Cosmogenesis.Core.Attributes;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Assembly, AllowMultiple = true)]
/// <summary>
/// Specifies in which database a document exists.
/// </summary>
public sealed class DbAttribute(string name) : Attribute
{
    public readonly string Name = name;
    public string? Namespace { get; set; }
}

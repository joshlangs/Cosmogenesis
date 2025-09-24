using Cosmogenesis.Generator.Models.Attributes;
using Microsoft.CodeAnalysis;

namespace Cosmogenesis.Generator.Models;
class ClassModel
{
    public INamedTypeSymbol ClassSymbol = default!;
    public readonly List<DbAttributeModel> DbAttributes = [];
    public DocTypeAttributeModel? DocTypeAttribute;
    public MutableAttributeModel? MutableAttribute;
    public PartitionAttributeModel? PartitionAttribute;
    public PartitionDefinitionAttributeModel? PartitionDefinitionAttribute;
    public TransientAttributeModel? TransientAttribute;
    public readonly List<PropertyModel> Properties = [];
    public readonly List<MethodModel> Methods = [];
    public bool IsDbDoc;
}

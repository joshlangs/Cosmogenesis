using Cosmogenesis.Generator.Models.Attributes;
using Microsoft.CodeAnalysis;

namespace Cosmogenesis.Generator.Models;
class OutputModel
{
    public bool CanGenerate = true;
    public SourceProductionContext Context;
    public CancellationToken CancellationToken;
    public Compilation Compilation = default!;
    public INamedTypeSymbol DbAttributeSymbol = default!;
    public INamedTypeSymbol DocTypeAttributeSymbol = default!;
    public INamedTypeSymbol MutableAttributeSymbol = default!;
    public INamedTypeSymbol PartitionAttributeSymbol = default!;
    public INamedTypeSymbol PartitionDefinitionAttributeSymbol = default!;
    public INamedTypeSymbol TransientAttributeSymbol = default!;
    public INamedTypeSymbol UseDefaultAttributeSymbol = default!;
    public INamedTypeSymbol DbDocSymbol = default!;
    public INamedTypeSymbol? JsonIgnoreAttributeSymbol = default!;
    public readonly List<ClassModel> Classes = [];
    public readonly List<DbAttributeModel> DbAttributes = [];
}

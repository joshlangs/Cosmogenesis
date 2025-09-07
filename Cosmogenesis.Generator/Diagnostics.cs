using Microsoft.CodeAnalysis;

namespace Cosmogenesis.Generator;
static class Diagnostics
{
    public static class Errors
    {
        static DiagnosticDescriptor Create(string id, string text) => new(id, text, text, "SourceGeneration", DiagnosticSeverity.Error, true);

        public static readonly DiagnosticDescriptor PartitionDefinitionStatic = Create("GEN001", "[PartitionDefinition] can only be attached to static classes");
        public static readonly DiagnosticDescriptor InvalidIdentifier = Create("GEN002", "The string {0} is not a valid identifier");
        public static readonly DiagnosticDescriptor Uppercase = Create("GEN003", "The identifier {0} should begin with an uppercase character");
        public static readonly DiagnosticDescriptor DbMultipleNamespace = Create("GEN004", "A database with this name was already defined in a different namespace");
        public static readonly DiagnosticDescriptor NoDatabase = Create("GEN005", "No matching database has been defined");
        public static readonly DiagnosticDescriptor PartitionAlreadyDefined = Create("GEN006", "A partition with this name has already been defined");        
        public static readonly DiagnosticDescriptor DocTypeDbDoc = Create("GEN007", "[DocType] can only decorate classes derived from DbDoc");
        public static readonly DiagnosticDescriptor PartitionDbDoc = Create("GEN008", "[Partition] can only decorate classes derived from DbDoc");
        public static readonly DiagnosticDescriptor MutableDbDoc = Create("GEN009", "[Mutable] can only decorate classes derived from DbDoc");
        public static readonly DiagnosticDescriptor TransientDbDoc = Create("GEN010", "[Transient] can only decorate classes derived from DbDoc");
        public static readonly DiagnosticDescriptor UseDefaultStatic = Create("GEN011", "[UseDefault] cannot be used on a static property");
        public static readonly DiagnosticDescriptor DocTypeAbstract = Create("GEN012", "[DocType] cannot be used on an abstract class");
        public static readonly DiagnosticDescriptor NoGetPk = Create("GEN013", "No accessible static method named 'GetPk' returning a string was found");
        public static readonly DiagnosticDescriptor NoGetId = Create("GEN014", "No accessible static method named 'GetId' returning a string was found");
        public static readonly DiagnosticDescriptor InvalidDocType = Create("GEN015", "The string {0} is not a valid document type. It should not be empty, nor contain leading or trailing whitespace, nor contain single or double quote characters");
        public static readonly DiagnosticDescriptor DuplicateDocType = Create("GEN016", "The document type {0} has already been defined by another document");
        public static readonly DiagnosticDescriptor PropertyArgumentCollision = Create("GEN017", "The generated property argument name {0} collides with the generated argument name for another property in this document");
        public static readonly DiagnosticDescriptor DatabaseNamespaces = Create("GEN018", "Multiple databases cannot share the same namespace");
        public static readonly DiagnosticDescriptor PropertyResolvePkId = Create("GEN019", "The argument {0} could not be matched with any property in the document");
        public static readonly DiagnosticDescriptor PropertyResolvePkIdConsistency = Create("GEN020", "The argument {0} was matched to property {1} but property {1} is not found in all documents within the partition");
        public static readonly DiagnosticDescriptor UseDefaultDbDoc = Create("GEN021", "[UseDefault] can only decorate properties in classes derived from DbDoc");
        public static readonly DiagnosticDescriptor MultipleGetId = Create("GEN022", "More than 1 accessible static methods named 'GetId' returning a string were found");
        public static readonly DiagnosticDescriptor MultipleGetPk = Create("GEN023", "More than 1 accessible static methods named 'GetPk' returning a string were found");
        public static readonly DiagnosticDescriptor NullableContext = Create("GEN024", "The assembly should default to a nullable context (add <Nullable>enable</Nullable> in a PropertyGroup inside your .csproj)");
        public static readonly DiagnosticDescriptor UseDefaultNullable = Create("GEN025", "[UseDefault] cannot be used on a reference type unless it is nullable");
        public static readonly DiagnosticDescriptor ParameterlessConstructor = Create("GEN026", "Documents must have an accessible constructor with no parameters");
        public static readonly DiagnosticDescriptor InvalidAutoExpiryTtl = Create("GEN027", "Transient auto-expiry ttl must be either -1 or greater than zero");
    }
    public static class Warnings
    {
        static DiagnosticDescriptor Create(string id, string text) => new(id, text, text, "SourceGeneration", DiagnosticSeverity.Warning, true);

        public static readonly DiagnosticDescriptor UseDefaultIgnored = Create("WGEN001", "[UseDefault] has no effect");
        public static readonly DiagnosticDescriptor EmptyPartition = Create("WGEN002", "Partition {0} is empty and will be ignored");
        public static readonly DiagnosticDescriptor DbDocWithoutPartition = Create("WGEN003", "Class {0} derives from DbDoc but has not defined a partition, so it will be ignored");
        public static readonly DiagnosticDescriptor InitOnlyKey = Create("WGEN004", "Consider making {0} init-only, because document keys derivation depends on it");
        public static readonly DiagnosticDescriptor InitOnlyNotMutable = Create("WGEN005", "Consider making {0} init-only, because the document is not mutable");
    }
}

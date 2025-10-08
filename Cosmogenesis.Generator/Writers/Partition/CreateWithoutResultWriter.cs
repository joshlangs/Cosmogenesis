using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers.Partition;
static class CreateWithoutResultWriter
{
    public static void Write(OutputModel outputModel, DatabasePlan databasePlan, PartitionPlan partitionPlan)
    {
        var s = $@"
namespace {databasePlan.Namespace};

public class {partitionPlan.CreateWithoutResultClassName}
{{
    protected virtual {databasePlan.Namespace}.{partitionPlan.ClassName} {partitionPlan.ClassName} {{ get; }} = default!;

    /// <summary>Mocking constructor</summary>
    protected {partitionPlan.CreateWithoutResultClassName}() {{ }}

    internal protected {partitionPlan.CreateWithoutResultClassName}({databasePlan.Namespace}.{partitionPlan.ClassName} {partitionPlan.ClassNameArgument})
    {{
        System.ArgumentNullException.ThrowIfNull({partitionPlan.ClassNameArgument});
        this.{partitionPlan.ClassName} = {partitionPlan.ClassNameArgument};
    }}

{string.Concat(partitionPlan.Documents.Select(x => CreateWithoutResult(partitionPlan, x)))}
}}
";

        outputModel.Context.AddSource($"partition_{partitionPlan.CreateWithoutResultClassName}.cs", s);
    }

    static string CreateWithoutResult(PartitionPlan partitionPlan, DocumentPlan documentPlan) => $@"
    /// <summary>
    /// Try to create a {documentPlan.ClassName} without the need to retrieve the saved document.
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    public virtual System.Threading.Tasks.Task<Cosmogenesis.Core.DbConflictType?> {documentPlan.ClassName}Async({documentPlan.PropertiesByName.Values.Where(x => !partitionPlan.GetPkPlan.ArgumentByPropertyName.ContainsKey(x.PropertyName)).AsInputParameters(documentPlan)}) => 
        this.{partitionPlan.ClassName}.CreateWithoutResultAsync({documentPlan.ClassNameArgument}: new {documentPlan.FullTypeName} {{ {partitionPlan.AsSettersFromDocumentPlanAndPartitionClass(documentPlan)} }});
";
}

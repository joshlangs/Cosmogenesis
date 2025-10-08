using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers.Partition;
static class CreateOrReplaceWithoutResultWriter
{
    public static void Write(OutputModel outputModel, DatabasePlan databasePlan, PartitionPlan partitionPlan)
    {
        if (!partitionPlan.Documents.Any(x => x.IsMutable || x.IsTransient))
        {
            return;
        }

        var s = $@"
namespace {databasePlan.Namespace};

public class {partitionPlan.CreateOrReplaceWithoutResultClassName}
{{
    protected virtual {databasePlan.Namespace}.{partitionPlan.ClassName} {partitionPlan.ClassName} {{ get; }} = default!;

    /// <summary>Mocking constructor</summary>
    protected {partitionPlan.CreateOrReplaceWithoutResultClassName}() {{ }}

    internal protected {partitionPlan.CreateOrReplaceWithoutResultClassName}({databasePlan.Namespace}.{partitionPlan.ClassName} {partitionPlan.ClassNameArgument})
    {{
        System.ArgumentNullException.ThrowIfNull({partitionPlan.ClassNameArgument});
        this.{partitionPlan.ClassName} = {partitionPlan.ClassNameArgument};
    }}

{string.Concat(partitionPlan.Documents.Select(x => CreateOrReplaceWithoutResult(partitionPlan, x)))}
}}
";

        outputModel.Context.AddSource($"partition_{partitionPlan.CreateOrReplaceWithoutResultClassName}.cs", s);
    }

    static string CreateOrReplaceWithoutResult(PartitionPlan partitionPlan, DocumentPlan documentPlan) =>
        !documentPlan.IsTransient && !documentPlan.IsMutable
        ? ""
        : $@"
    /// <summary>
    /// Create or replace (unconditionally overwrite) a {documentPlan.ClassName} without the need to retrieve the saved document.
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    public virtual System.Threading.Tasks.Task {documentPlan.ClassName}Async({documentPlan.PropertiesByName.Values.Where(x => !partitionPlan.GetPkPlan.ArgumentByPropertyName.ContainsKey(x.PropertyName)).AsInputParameters(documentPlan)}) => 
        this.{partitionPlan.ClassName}.CreateOrReplaceWithoutResultAsync(new {documentPlan.FullTypeName} {{ {partitionPlan.AsSettersFromDocumentPlanAndPartitionClass(documentPlan)} }});
";
}

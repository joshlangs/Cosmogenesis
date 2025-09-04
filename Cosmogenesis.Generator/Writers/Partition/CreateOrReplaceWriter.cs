using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers.Partition;
static class CreateOrReplaceWriter
{
    public static void Write(OutputModel outputModel, DatabasePlan databasePlan, PartitionPlan partitionPlan)
    {
        if (!partitionPlan.Documents.Any(x => x.IsMutable || x.IsTransient))
        {
            return;
        }

        var s = $@"
namespace {databasePlan.Namespace};

public class {partitionPlan.CreateOrReplaceClassName}
{{
    protected virtual {databasePlan.Namespace}.{partitionPlan.ClassName} {partitionPlan.ClassName} {{ get; }} = default!;

    /// <summary>Mocking constructor</summary>
    protected {partitionPlan.CreateOrReplaceClassName}() {{ }}

    internal protected {partitionPlan.CreateOrReplaceClassName}({databasePlan.Namespace}.{partitionPlan.ClassName} {partitionPlan.ClassNameArgument})
    {{
        this.{partitionPlan.ClassName} = {partitionPlan.ClassNameArgument} ?? throw new System.ArgumentNullException(nameof({partitionPlan.ClassNameArgument}));
    }}

{string.Concat(partitionPlan.Documents.Select(x => CreateOrReplace(partitionPlan, x)))}
}}
";

        outputModel.Context.AddSource($"partition_{partitionPlan.CreateOrReplaceClassName}.cs", s);
    }

    static string CreateOrReplace(PartitionPlan partitionPlan, DocumentPlan documentPlan) =>
        !documentPlan.IsTransient && !documentPlan.IsMutable
        ? ""
        : $@"
    /// <summary>
    /// Create or replace (unconditionally overwrite) a {documentPlan.ClassName}.
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    public virtual System.Threading.Tasks.Task<Cosmogenesis.Core.CreateOrReplaceResult<{documentPlan.FullTypeName}>> {documentPlan.ClassName}Async({documentPlan.PropertiesByName.Values.Where(x => !partitionPlan.GetPkPlan.ArgumentByPropertyName.ContainsKey(x.PropertyName)).AsInputParameters(documentPlan)}) => 
        this.{partitionPlan.ClassName}.CreateOrReplaceAsync(new {documentPlan.FullTypeName} {{ {partitionPlan.AsSettersFromDocumentPlanAndPartitionClass(documentPlan)} }});
";
}

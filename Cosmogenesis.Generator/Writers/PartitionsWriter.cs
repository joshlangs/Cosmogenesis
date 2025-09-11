using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;
using Cosmogenesis.Generator.Writers.Partition;

namespace Cosmogenesis.Generator.Writers;
static class PartitionsWriter
{
    public static void Write(OutputModel outputModel, DatabasePlan databasePlan)
    {
        var s = $@"
namespace {databasePlan.Namespace};

public class {databasePlan.PartitionsClassName}
{{
    protected virtual {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassName} {{ get; }} = default!;

    /// <summary>Mocking constructor</summary>
    protected {databasePlan.PartitionsClassName}() {{ }}

    internal protected {databasePlan.PartitionsClassName}({databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassNameArgument})
    {{
        System.ArgumentNullException.ThrowIfNull({databasePlan.DbClassNameArgument});
        this.{databasePlan.DbClassName} = {databasePlan.DbClassNameArgument};
    }}

{string.Concat(databasePlan.PartitionPlansByName.Values.Select(x => Partition(databasePlan, x)))}
}}
";
        outputModel.Context.AddSource($"db_{databasePlan.PartitionsClassName}.cs", s);

        foreach (var partition in databasePlan.PartitionPlansByName.Values)
        {
            PartitionWriter.Write(outputModel, databasePlan, partition);
        }
    }

    static string Partition(DatabasePlan databasePlan, PartitionPlan partitionPlan) => $@"
    public virtual {databasePlan.Namespace}.{partitionPlan.ClassName} {partitionPlan.Name}({partitionPlan.GetPkPlan.AsInputParameters()}) =>
        new({new[] { CreateClassParameter(databasePlan), CreateKeyParameter(partitionPlan) }.JoinNonEmpty()});
";

    static string CreateClassParameter(DatabasePlan databasePlan) => $@"
            {databasePlan.DbClassNameArgument}: this.{databasePlan.DbClassName}";

    static string CreateKeyParameter(PartitionPlan partitionPlan) =>
        partitionPlan.GetPkPlan.Arguments.Count == 0 ?
        "" : $@"
            pkData: new() {{ {partitionPlan.GetPkPlan.AsSettersFromParameters()} }}";
}

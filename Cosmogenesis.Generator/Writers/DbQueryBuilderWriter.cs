using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers;
static class DbQueryBuilderWriter
{
    public static void Write(OutputModel outputModel, DatabasePlan databasePlan)
    {
        var anyUnions = databasePlan.PartitionPlansByName.Values.SelectMany(x => x.Unions).Any();

        var s = $@"
namespace {databasePlan.Namespace};

public class {databasePlan.QueryBuilderClassName} : Cosmogenesis.Core.DbQueryBuilderBase
{{
    protected virtual {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassName} {{ get; }} = default!;

    /// <summary>Mocking constructor</summary>
    protected {databasePlan.QueryBuilderClassName}() {{ }}

    internal protected {databasePlan.QueryBuilderClassName}(
        {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassNameArgument})
        : base(
            dbBase: {databasePlan.DbClassNameArgument},
            partitionKey: null)
    {{
        System.ArgumentNullException.ThrowIfNull({databasePlan.DbClassNameArgument});
        {databasePlan.DbClassName} = {databasePlan.DbClassNameArgument};
    }}

{string.Concat(databasePlan.PartitionPlansByName.Values.SelectMany(x => x.Documents).Select(BuildQuery))}
{(anyUnions ? Unions(databasePlan) : "")}
}}
";
        
        if (anyUnions)
        {
            DbQueryBuilderUnionsWriter.Write(outputModel, databasePlan);
        }

        outputModel.Context.AddSource($"db_{databasePlan.QueryBuilderClassName}.cs", s);
    }

    static string Unions(DatabasePlan databasePlan) => $@"
    {databasePlan.Namespace}.{databasePlan.QueryBuilderUnionsClassName}? unions;
    public virtual {databasePlan.Namespace}.{databasePlan.QueryBuilderUnionsClassName} Unions => this.unions ??= new(
        {databasePlan.DbClassNameArgument}: this.{databasePlan.DbClassName});
";

    static string BuildQuery(DocumentPlan documentPlan) => $@"
    /// <summary>
    /// Build a query filtered to {documentPlan.ClassName} documents.
    /// Additional Linq transformations can be appended.
    /// Use ExecuteQueryAsync to execute.
    /// <see cref=""https://github.com/Azure/azure-cosmos-dotnet-v3/blob/bb72ba5786d99d928b4774e16810f2655029e8a2/Microsoft.Azure.Cosmos/src/Linq/CosmosLinqExtensions.cs"" />
    /// </summary>
    public virtual System.Linq.IQueryable<{documentPlan.FullTypeName}> {documentPlan.PluralName}() => 
        this.BuildQueryByType<{documentPlan.FullTypeName}>(type: {documentPlan.ConstDocType});
";
}

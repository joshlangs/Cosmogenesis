using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers.Partition;
static class QueryBuilderWriter
{
    public static void Write(OutputModel outputModel, DatabasePlan databasePlan, PartitionPlan partitionPlan)
    {
        var s = $@"
namespace {databasePlan.Namespace};

public class {partitionPlan.QueryBuilderClassName} : Cosmogenesis.Core.DbQueryBuilderBase
{{
    protected virtual {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassName} {{ get; }} = default!;
    
    /// <summary>Mocking constructor</summary>
    protected {partitionPlan.QueryBuilderClassName}() {{ }}

    internal protected {partitionPlan.QueryBuilderClassName}(
        {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassNameArgument},
        Microsoft.Azure.Cosmos.PartitionKey? partitionKey)
        : base(
            dbBase: {databasePlan.DbClassNameArgument},
            partitionKey: partitionKey)
    {{
        System.ArgumentNullException.ThrowIfNull({databasePlan.DbClassNameArgument});
        {databasePlan.DbClassName} = {databasePlan.DbClassNameArgument};
    }}

{string.Concat(partitionPlan.Documents.Select(BuildQuery))}
{Unions(databasePlan, partitionPlan)}
}}
";

        if (partitionPlan.Unions.Any())
        {
            QueryBuilderUnionsWriter.Write(outputModel, databasePlan, partitionPlan);
        }

        outputModel.Context.AddSource($"partition_{partitionPlan.QueryBuilderClassName}.cs", s);
    }

    static string Unions(DatabasePlan databasePlan, PartitionPlan partitionPlan) =>
        partitionPlan.Unions.Count == 0 ? "" : $@"
    {databasePlan.Namespace}.{partitionPlan.QueryBuilderUnionsClassName}? unions;
    public virtual {databasePlan.Namespace}.{partitionPlan.QueryBuilderUnionsClassName} Unions => this.unions ??= new(
        {databasePlan.DbClassNameArgument}: this.{databasePlan.DbClassName},
        partitionKey: this.PartitionKey);
";

    static string BuildQuery(DocumentPlan documentPlan) => $@"
    /// <summary>
    /// Build a query filtered to {documentPlan.ClassName} documents.
    /// Additional Linq transformations can be appended.
    /// Use ExecuteQueryAsync to execute.
    /// <see cref=""https://github.com/Azure/azure-cosmos-dotnet-v3/blob/bb72ba5786d99d928b4774e16810f2655029e8a2/Microsoft.Azure.Cosmos/src/Linq/CosmosLinqExtensions.cs"" />
    /// </summary>
    public virtual System.Linq.IQueryable<{documentPlan.FullTypeName}> {documentPlan.ClassName.Pluralize()}() => 
        this.BuildQueryByType<{documentPlan.FullTypeName}>(type: {documentPlan.ConstDocType});
";
}

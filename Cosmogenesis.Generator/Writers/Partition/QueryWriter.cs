using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers.Partition;
static class QueryWriter
{
    public static void Write(OutputModel outputModel, DatabasePlan databasePlan, PartitionPlan partitionPlan)
    {
        var s = $@"
namespace {databasePlan.Namespace};

public class {partitionPlan.QueryClassName} : Cosmogenesis.Core.DbQueryBase
{{
    protected virtual {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassName} {{ get; }} = default!;
    protected virtual {databasePlan.Namespace}.{partitionPlan.QueryBuilderClassName} {partitionPlan.QueryBuilderClassName} {{ get; }} = default!;

    /// <summary>Mocking constructor</summary>
    protected {partitionPlan.QueryClassName}() {{ }}

    internal protected {partitionPlan.QueryClassName}(
        {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassNameArgument},
        {databasePlan.Namespace}.{partitionPlan.QueryBuilderClassName} {partitionPlan.QueryBuilderClassNameArgument})
        : base(
            dbBase: {databasePlan.DbClassNameArgument},
            queryBuilder: {partitionPlan.QueryBuilderClassNameArgument})
    {{
        System.ArgumentNullException.ThrowIfNull({databasePlan.DbClassNameArgument});
        System.ArgumentNullException.ThrowIfNull({partitionPlan.QueryBuilderClassNameArgument});
        this.{databasePlan.DbClassName} = {databasePlan.DbClassNameArgument};
        this.{partitionPlan.QueryBuilderClassName} = {partitionPlan.QueryBuilderClassNameArgument};
    }}

{string.Concat(partitionPlan.Documents.Select(x => Query(databasePlan, partitionPlan, x)))}
{Unions(databasePlan, partitionPlan)}
}}
"; 
        
        if (partitionPlan.Unions.Any())
        {
            QueryUnionsWriter.Write(outputModel, databasePlan, partitionPlan);
        }

        outputModel.Context.AddSource($"partition_{partitionPlan.QueryClassName}.cs", s);
    }

    static string Unions(DatabasePlan databasePlan, PartitionPlan partitionPlan) =>
        partitionPlan.Unions.Count == 0 ? "" : $@"
    {databasePlan.Namespace}.{partitionPlan.QueryUnionsClassName}? unions;
    public virtual {databasePlan.Namespace}.{partitionPlan.QueryUnionsClassName} Unions => this.unions ??= new(
        {databasePlan.DbClassNameArgument}: this.{databasePlan.DbClassName},
        {partitionPlan.QueryBuilderClassNameArgument}: this.{partitionPlan.QueryBuilderClassName});
";

    static string Query(DatabasePlan databasePlan, PartitionPlan partitionPlan, DocumentPlan documentPlan) => $@"
    /// <summary>
    /// Build and execute a query filtered to {documentPlan.ClassName} documents.
    /// <see cref=""https://github.com/Azure/azure-cosmos-dotnet-v3/blob/bb72ba5786d99d928b4774e16810f2655029e8a2/Microsoft.Azure.Cosmos/src/Linq/CosmosLinqExtensions.cs"" />
    /// </summary>
    public virtual System.Collections.Generic.IAsyncEnumerable<T> {documentPlan.ClassName.Pluralize()}<T>(
        System.Func<System.Linq.IQueryable<{documentPlan.FullTypeName}>, System.Linq.IQueryable<T>> createQuery, 
        System.Threading.CancellationToken cancellationToken = default)
        => this.{databasePlan.DbClassName}
            .ExecuteQueryAsync(
                query: createQuery(this.{partitionPlan.QueryBuilderClassName}.{documentPlan.ClassName.Pluralize()}()),
                cancellationToken: cancellationToken);

    /// <summary>
    /// Execute a query filtered to {documentPlan.ClassName} documents.
    /// </summary>
    public virtual System.Collections.Generic.IAsyncEnumerable<{documentPlan.FullTypeName}> {documentPlan.ClassName.Pluralize()}(
        System.Threading.CancellationToken cancellationToken = default)
        => this.{databasePlan.DbClassName}
            .ExecuteQueryAsync(
                query: this.{partitionPlan.QueryBuilderClassName}.{documentPlan.ClassName.Pluralize()}(),
                cancellationToken: cancellationToken);
";
}

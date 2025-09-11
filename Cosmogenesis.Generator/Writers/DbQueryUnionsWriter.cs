using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers;
static class DbQueryUnionsWriter
{
    public static void Write(OutputModel outputModel, DatabasePlan databasePlan)
    {
        var s = $@"
namespace {databasePlan.Namespace};

public class {databasePlan.QueryUnionsClassName} : Cosmogenesis.Core.DbQueryBase
{{
    protected virtual {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassName} {{ get; }} = default!;
    protected virtual {databasePlan.Namespace}.{databasePlan.QueryBuilderClassName} {databasePlan.QueryBuilderClassName} {{ get; }} = default!;

    /// <summary>Mocking constructor</summary>
    protected {databasePlan.QueryUnionsClassName}() {{ }}

    internal protected {databasePlan.QueryUnionsClassName}(
        {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassNameArgument},
        {databasePlan.Namespace}.{databasePlan.QueryBuilderClassName} {databasePlan.QueryBuilderClassNameArgument})
        : base(
            dbBase: {databasePlan.DbClassNameArgument},
            queryBuilder: {databasePlan.QueryBuilderClassNameArgument})
    {{
        System.ArgumentNullException.ThrowIfNull({databasePlan.DbClassNameArgument});
        System.ArgumentNullException.ThrowIfNull({databasePlan.QueryBuilderClassNameArgument});
        this.{databasePlan.DbClassName} = {databasePlan.DbClassNameArgument};
        this.{databasePlan.QueryBuilderClassName} = {databasePlan.QueryBuilderClassNameArgument};
    }}

{string.Concat(databasePlan.PartitionPlansByName.Values.SelectMany(x => x.Unions).Select(x => Query(databasePlan, x)))}
}}
";

        outputModel.Context.AddSource($"db_{databasePlan.QueryUnionsClassName}.cs", s);
    }

    static string Query(DatabasePlan databasePlan, UnionPlan unionPlan) => $@"
    /// <summary>
    /// Build and execute a query filtered to {unionPlan.CommonName} documents.
    /// {unionPlan.CommonName} is a union of: {string.Join(", ", unionPlan.Documents.Select(x => x.ClassName))}
    /// <see cref=""https://github.com/Azure/azure-cosmos-dotnet-v3/blob/bb72ba5786d99d928b4774e16810f2655029e8a2/Microsoft.Azure.Cosmos/src/Linq/CosmosLinqExtensions.cs"" />
    /// </summary>
    public virtual System.Collections.Generic.IAsyncEnumerable<T> {unionPlan.CommonName.Pluralize()}<T>(
        System.Func<System.Linq.IQueryable<{unionPlan.FullCommonTypeName}>, System.Linq.IQueryable<T>> createQuery,
        System.Threading.CancellationToken cancellationToken = default) 
        => this.{databasePlan.DbClassName}
            .ExecuteQueryAsync(
                query: createQuery(this.{databasePlan.QueryBuilderClassName}.Unions.{unionPlan.CommonName.Pluralize()}()),
                cancellationToken: cancellationToken);

    /// <summary>
    /// Execute a query filtered to {unionPlan.CommonName} documents.
    /// {unionPlan.CommonName} is a union of: {string.Join(", ", unionPlan.Documents.Select(x => x.ClassName))}
    /// </summary>
    public virtual System.Collections.Generic.IAsyncEnumerable<{unionPlan.FullCommonTypeName}> {unionPlan.CommonName.Pluralize()}(
        System.Threading.CancellationToken cancellationToken = default)
        => this.{databasePlan.DbClassName}
            .ExecuteQueryAsync(
                query: this.{databasePlan.QueryBuilderClassName}.Unions.{unionPlan.CommonName.Pluralize()}(),
                cancellationToken: cancellationToken);
";
}

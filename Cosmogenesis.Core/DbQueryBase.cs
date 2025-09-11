namespace Cosmogenesis.Core;

public abstract class DbQueryBase
{
    protected virtual DbBase DbBase { get; } = default!;
    protected virtual DbQueryBuilderBase QueryBuilder { get; } = default!;

    protected DbQueryBase() { }

    protected DbQueryBase(DbBase dbBase, DbQueryBuilderBase queryBuilder)
    {
        ArgumentNullException.ThrowIfNull(dbBase);
        ArgumentNullException.ThrowIfNull(queryBuilder);
        DbBase = dbBase;
        QueryBuilder = queryBuilder;
    }

    /// <summary>
    /// Build and execute a query covering all documents.
    /// Example 1: `x => x["ActualFieldName"] == 4`
    /// Example 2: `x => ((SomeDbDoc)(object)x).ActualFieldName == 4`
    /// <see cref=""https://github.com/Azure/azure-cosmos-dotnet-v3/blob/bb72ba5786d99d928b4774e16810f2655029e8a2/Microsoft.Azure.Cosmos/src/Linq/CosmosLinqExtensions.cs"" />
    /// </summary>
    public virtual IAsyncEnumerable<T> Dynamic<T>(
        Func<IQueryable<IDictionary<string, object>>, IQueryable<T>> createQuery,
        CancellationToken cancellationToken = default) => DbBase
            .ExecuteQueryAsync(
                query: createQuery(QueryBuilder.Dynamic()),
                cancellationToken: cancellationToken);

    /// <summary>
    /// Execute a query covering all documents.
    /// </summary>
    public virtual IAsyncEnumerable<DbDoc> Dynamic(CancellationToken cancellationToken = default) => DbBase
        .ExecuteQueryAsync(
            query: QueryBuilder.Dynamic().AsKnownDocument(),
            cancellationToken: cancellationToken);
}

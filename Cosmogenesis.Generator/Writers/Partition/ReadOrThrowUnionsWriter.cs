using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers.Partition;
class ReadOrThrowUnionsWriter
{
    public static void Write(OutputModel outputModel, DatabasePlan databasePlan, PartitionPlan partitionPlan)
    {
        var s = $@"
namespace {databasePlan.Namespace};

public class {partitionPlan.ReadOrThrowUnionsClassName}
{{
    protected virtual {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassName} {{ get; }} = default!;
    protected virtual Microsoft.Azure.Cosmos.PartitionKey PartitionKey {{ get; }} = default!;

    /// <summary>Mocking constructor</summary>
    protected {partitionPlan.ReadOrThrowUnionsClassName}() {{ }}

    internal protected {partitionPlan.ReadOrThrowUnionsClassName}(
        {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassNameArgument},
        Microsoft.Azure.Cosmos.PartitionKey partitionKey)
    {{
        System.ArgumentNullException.ThrowIfNull({databasePlan.DbClassNameArgument});
        this.{databasePlan.DbClassName} = {databasePlan.DbClassNameArgument};
        this.PartitionKey = partitionKey;
    }}

{string.Concat(partitionPlan.Unions.Select(x => ReadOrThrow(databasePlan, x)))}
}}
";

        outputModel.Context.AddSource($"partition_{partitionPlan.ReadOrThrowUnionsClassName}.cs", s);
    }

    static string ReadOrThrow(DatabasePlan databasePlan, UnionPlan unionPlan) => $@"
    /// <summary>
    /// Try to load a {unionPlan.CommonName} by id.
    /// id should be transformed using Cosmogenesis.Core.DbDocHelper.GetValidId.
    /// Returns the {unionPlan.CommonName} or throws DbConflictException if not found.
    /// {unionPlan.CommonName} is a union of: {string.Join(", ", unionPlan.Documents.Select(x => x.ClassName))}
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbConflictException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    protected virtual async System.Threading.Tasks.Task<{unionPlan.FullCommonTypeName}> {unionPlan.CommonName}Async(string id)
    {{
        var result = ({unionPlan.FullCommonTypeName}?)await this.{databasePlan.DbClassName}.ReadByIdAsync(
            partitionKey: this.PartitionKey,
            id: id);
        if (result is null)
        {{
            throw Cosmogenesis.Core.DbModelFactory.CreateDbConflictException(dbConflictType: Cosmogenesis.Core.DbConflictType.Missing);
        }}
        return result;
    }}

    /// <summary>
    /// Try to load a {unionPlan.CommonName} by id.
    /// Returns the {unionPlan.CommonName} or throws DbConflictException if not found.
    /// {unionPlan.CommonName} is a union of: {string.Join(", ", unionPlan.Documents.Select(x => x.ClassName))}
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbConflictException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    public virtual async System.Threading.Tasks.Task<{unionPlan.FullCommonTypeName}> {unionPlan.CommonName}Async({unionPlan.GetIdPlan.AsInputParameters()})
    {{
        var result = ({unionPlan.FullCommonTypeName}?)await this.{databasePlan.DbClassName}.ReadByIdAsync(
            partitionKey: this.PartitionKey,
            id: Cosmogenesis.Core.DbDocHelper.GetValidId({unionPlan.GetIdPlan.FullMethodName}({unionPlan.GetIdPlan.AsInputParameterMapping()})));
        if (result is null)
        {{
            throw Cosmogenesis.Core.DbModelFactory.CreateDbConflictException(dbConflictType: Cosmogenesis.Core.DbConflictType.Missing);
        }}
        return result;
    }}
";
}

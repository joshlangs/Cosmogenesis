using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers.Partition;
class ReadManyUnionsWriter
{
    public static void Write(OutputModel outputModel, DatabasePlan databasePlan, PartitionPlan partitionPlan)
    {
        var s = $@"
using System.Linq;
namespace {databasePlan.Namespace};

public class {partitionPlan.ReadManyUnionsClassName}
{{
    protected virtual {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassName} {{ get; }} = default!;
    protected virtual Microsoft.Azure.Cosmos.PartitionKey PartitionKey {{ get; }} = default!;

    /// <summary>Mocking constructor</summary>
    protected {partitionPlan.ReadManyUnionsClassName}() {{ }}

    internal protected {partitionPlan.ReadManyUnionsClassName}(
        {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassNameArgument},
        Microsoft.Azure.Cosmos.PartitionKey partitionKey)
    {{
        System.ArgumentNullException.ThrowIfNull({databasePlan.DbClassNameArgument});
        this.{databasePlan.DbClassName} = {databasePlan.DbClassNameArgument};
        this.PartitionKey = partitionKey;
    }}

{string.Concat(partitionPlan.Unions.Where(x => x.GetIdPlan.Arguments.Any()).Select(x => ReadById(databasePlan, x)))}
{string.Concat(partitionPlan.Unions.Where(x => x.GetIdPlan.Arguments.Any()).Select(x => Read(databasePlan, x)))}
}}";

        outputModel.Context.AddSource($"partition_{partitionPlan.ReadManyUnionsClassName}.cs", s);
    }

    static string ReadById(DatabasePlan databasePlan, UnionPlan unionPlan) => $@"
    /// <summary>
    /// Try to load {unionPlan.CommonName} documents by id.
    /// id should be transformed using Cosmogenesis.Core.DbDocHelper.GetValidId.
    /// Returns an array of {unionPlan.CommonName} documents (or null if not found) in the same order as the ids were provided.
    /// {unionPlan.CommonName} is a union of: {string.Join(", ", unionPlan.Documents.Select(x => x.ClassName))}
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    protected virtual async System.Threading.Tasks.Task<{unionPlan.FullCommonTypeName}?[]> {unionPlan.CommonName.Pluralize()}ByIdAsync(System.Collections.Generic.IEnumerable<string> ids)
    {{
        var docs = await this.{databasePlan.DbClassName}.ReadByIdsAsync(
            partitionKey: this.PartitionKey, 
            ids: ids);
        var results = new {unionPlan.FullCommonTypeName}?[docs.Length];
        for (var x = 0; x < docs.Length; ++x)
        {{
            results[x] = ({unionPlan.FullCommonTypeName}?)docs[x];
        }}
        return results;
    }}
";

    static string Read(DatabasePlan databasePlan, UnionPlan unionPlan)
    {
        var singleType = unionPlan.GetIdPlan.Arguments[0].FullTypeName;
        var singleTypeParam = unionPlan.GetIdPlan.Arguments[0].ArgumentName.Pluralize();

        var inputParams =
            unionPlan.GetIdPlan.Arguments.Count == 1
            ? $"System.Collections.Generic.IEnumerable<{singleType}> {singleTypeParam}"
            : $"System.Collections.Generic.IEnumerable<({unionPlan.GetIdPlan.AsInputParameters()})> ids";
        var toId =
            unionPlan.GetIdPlan.Arguments.Count == 1
            ? $"{singleTypeParam}.Select({unionPlan.GetIdPlan.FullMethodName}).Select(Cosmogenesis.Core.DbDocHelper.GetValidId)"
            : $"ids.Select(x => {unionPlan.GetIdPlan.FullMethodName}({unionPlan.GetIdPlan.ParametersToParametersMapping("x")})).Select(Cosmogenesis.Core.DbDocHelper.GetValidId)";
        return $@"
    /// <summary>
    /// Try to load {unionPlan.CommonName} documents by id.
    /// Returns an array of {unionPlan.CommonName} documents (or null if not found) in the same order as the ids were provided.
    /// {unionPlan.CommonName} is a union of: {string.Join(", ", unionPlan.Documents.Select(x => x.ClassName))}
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    public virtual async System.Threading.Tasks.Task<{unionPlan.FullCommonTypeName}?[]> {unionPlan.CommonName.Pluralize()}Async({inputParams})
    {{
        var docs = await this.{databasePlan.DbClassName}.ReadByIdsAsync(
            partitionKey: this.PartitionKey, 
            ids: {toId});
        var results = new {unionPlan.FullCommonTypeName}?[docs.Length];
        for (var x = 0; x < docs.Length; ++x)
        {{
            results[x] = ({unionPlan.FullCommonTypeName}?)docs[x];
        }}
        return results;
    }}
";
    }
}

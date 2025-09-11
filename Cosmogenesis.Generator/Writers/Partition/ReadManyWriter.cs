using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers.Partition;
static class ReadManyWriter
{
    public static void Write(OutputModel outputModel, DatabasePlan databasePlan, PartitionPlan partitionPlan)
    {
        if (!partitionPlan.Documents.Any(x => x.GetIdPlan.Arguments.Count > 0))
        {
            return;
        }

        var anyUnions = partitionPlan.Unions.Where(x => x.GetIdPlan.Arguments.Any()).Any();

        var s = $@"
using System.Linq;
namespace {databasePlan.Namespace};

public class {partitionPlan.ReadManyClassName}
{{
    protected virtual {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassName} {{ get; }} = default!;
    protected virtual Microsoft.Azure.Cosmos.PartitionKey PartitionKey {{ get; }} = default!;

    /// <summary>Mocking constructor</summary>
    protected {partitionPlan.ReadManyClassName}() {{ }}

    internal protected {partitionPlan.ReadManyClassName}(
        {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassNameArgument},
        Microsoft.Azure.Cosmos.PartitionKey partitionKey)
    {{
        System.ArgumentNullException.ThrowIfNull({databasePlan.DbClassNameArgument});
        this.{databasePlan.DbClassName} = {databasePlan.DbClassNameArgument};
        this.PartitionKey = partitionKey;
    }}

{string.Concat(partitionPlan.Documents.Select(x => ReadById(databasePlan, x)))}
{string.Concat(partitionPlan.Documents.Select(x => Read(databasePlan, x)))}
{(anyUnions ? Unions(databasePlan, partitionPlan) : "")}
}}
";

        if (anyUnions)
        {
            ReadManyUnionsWriter.Write(outputModel, databasePlan, partitionPlan);
        }

        outputModel.Context.AddSource($"partition_{partitionPlan.ReadManyClassName}.cs", s);
    }

    static string Unions(DatabasePlan databasePlan, PartitionPlan partitionPlan) =>
        partitionPlan.Unions.Count == 0 ? "" : $@"
    {databasePlan.Namespace}.{partitionPlan.ReadManyUnionsClassName}? unions;
    public virtual {databasePlan.Namespace}.{partitionPlan.ReadManyUnionsClassName} Unions => this.unions ??= new(
        {databasePlan.DbClassNameArgument}: this.{databasePlan.DbClassName},
        partitionKey: this.PartitionKey);
";

    static string ReadById(DatabasePlan databasePlan, DocumentPlan documentPlan) =>
        documentPlan.GetIdPlan.Arguments.Count == 0
        ? ""
        : $@"
    /// <summary>
    /// Try to load {documentPlan.ClassName} documents by id.
    /// id should be transformed using Cosmogenesis.Core.DbDocHelper.GetValidId.
    /// Returns an array of {documentPlan.ClassName} documents (or null if not found) in the same order as the ids were provided.
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    protected virtual System.Threading.Tasks.Task<{documentPlan.FullTypeName}?[]> {documentPlan.ClassName.Pluralize()}ByIdAsync(System.Collections.Generic.IEnumerable<string> ids) => 
        this.{databasePlan.DbClassName}.ReadByIdsAsync<{documentPlan.FullTypeName}>(
            partitionKey: this.PartitionKey, 
            ids: ids, 
            type: {documentPlan.ConstDocType});
";

    static string Read(DatabasePlan databasePlan, DocumentPlan documentPlan)
    {
        if (documentPlan.GetIdPlan.Arguments.Count == 0)
        {
            return "";
        }

        var singleType = documentPlan.GetIdPlan.Arguments[0].FullTypeName;
        var singleTypeParam = documentPlan.GetIdPlan.Arguments[0].ArgumentName.Pluralize();

        var inputParams =
            documentPlan.GetIdPlan.Arguments.Count == 1
            ? $"System.Collections.Generic.IEnumerable<{singleType}> {singleTypeParam}"
            : $"System.Collections.Generic.IEnumerable<({documentPlan.GetIdPlan.AsInputParameters()})> ids";
        var toId =
            documentPlan.GetIdPlan.Arguments.Count == 1
            ? $"{singleTypeParam}.Select({documentPlan.GetIdPlan.FullMethodName}).Select(Cosmogenesis.Core.DbDocHelper.GetValidId)"
            : $"ids.Select(x => {documentPlan.GetIdPlan.FullMethodName}({documentPlan.GetIdPlan.ParametersToParametersMapping("x")})).Select(Cosmogenesis.Core.DbDocHelper.GetValidId)";
        return $@"
    /// <summary>
    /// Try to load {documentPlan.ClassName} documents by id.
    /// Returns an array of {documentPlan.ClassName} documents (or null if not found) in the same order as the ids were provided.
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    public virtual System.Threading.Tasks.Task<{documentPlan.FullTypeName}?[]> {documentPlan.ClassName.Pluralize()}Async({inputParams}) => 
        this.{databasePlan.DbClassName}.ReadByIdsAsync<{documentPlan.FullTypeName}>(
            partitionKey: this.PartitionKey, 
            ids: {toId}, 
            type: {documentPlan.ConstDocType});
";
    }
}

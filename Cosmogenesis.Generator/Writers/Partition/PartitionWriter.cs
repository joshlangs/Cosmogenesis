using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers.Partition;
static class PartitionWriter
{
    public static void Write(OutputModel outputModel, DatabasePlan databasePlan, PartitionPlan partitionPlan)
    {
        var s = $@"
namespace {databasePlan.Namespace};

public class {partitionPlan.ClassName} : Cosmogenesis.Core.DbPartitionBase
{{
    protected virtual {databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassName} {{ get; }} = default!;

    {PkClass(partitionPlan)}

    /// <summary>Mocking constructor</summary>
    protected {partitionPlan.ClassName}() {{ }}

    internal protected {partitionPlan.ClassName}(
        {new[] { ConstructorClassParameter(databasePlan), ConstructorKeyParameter(partitionPlan) }.Where(x => !string.IsNullOrEmpty(x)).JoinNonEmpty()})
        : base(
            db: {databasePlan.DbClassNameArgument},
            partitionKey: {partitionPlan.GetPkPlan.FullMethodName}({partitionPlan.GetPkPlan.DocumentToParametersMapping("pkData")}),
            serializer: {databasePlan.Namespace}.{databasePlan.SerializerClassName}.Instance)
    {{
        System.ArgumentNullException.ThrowIfNull({databasePlan.DbClassNameArgument});
        this.{databasePlan.DbClassName} = {databasePlan.DbClassNameArgument};
        {PkClassSetter(partitionPlan)}
    }}

    {databasePlan.Namespace}.{partitionPlan.QueryBuilderClassName}? queryBuilder;
    /// <summary>
    /// Methods to build queries for later execution.
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.QueryBuilderClassName} QueryBuilder => this.queryBuilder ??= new(
        {databasePlan.DbClassNameArgument}: this.{databasePlan.DbClassName},
        partitionKey: this.PartitionKey);

    {databasePlan.Namespace}.{partitionPlan.QueryClassName}? query;
    /// <summary>
    /// Methods to execute queries.
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.QueryClassName} Query => this.query ??= new(
        {databasePlan.DbClassNameArgument}: this.{databasePlan.DbClassName},
        {partitionPlan.QueryBuilderClassNameArgument}: this.QueryBuilder);

    /// <summary>
    /// A batch of operations to be executed atomically (or not at all) within a {partitionPlan.Name} in the {databasePlan.Name} database.
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.BatchClassName} CreateBatch() => new(
        transactionalBatch: this.CreateBatchForPartition(),
        partitionKey: this.PartitionKeyString,
        validateStateBeforeSave: this.{databasePlan.DbClassName}.ValidateStateBeforeSave,
        {partitionPlan.ClassNameArgument}: this);

    {databasePlan.Namespace}.{partitionPlan.ReadClassName}? read;
    /// <summary>
    /// Methods to read documents.
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.ReadClassName} Read => this.read ??= new(
        {databasePlan.DbClassNameArgument}: this.{databasePlan.DbClassName}, 
        partitionKey: this.PartitionKey);

    {databasePlan.Namespace}.{partitionPlan.ReadOrThrowClassName}? readOrThrow;
    /// <summary>
    /// Methods to read documents, or throw DbConflictException is they are not found.
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.ReadOrThrowClassName} ReadOrThrow => this.readOrThrow ??= new(
        {databasePlan.DbClassNameArgument}: this.{databasePlan.DbClassName}, 
        partitionKey: this.PartitionKey);

    {databasePlan.Namespace}.{partitionPlan.CreateClassName}? create;
    /// <summary>
    /// Methods to create documents.
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.CreateClassName} Create => this.create ??= new(this);

    {databasePlan.Namespace}.{partitionPlan.ReadOrCreateClassName}? readOrCreate;
    /// <summary>
    /// Methods to read documents, or create them if they did not yet exist.
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.ReadOrCreateClassName} ReadOrCreate => this.readOrCreate ??= new(this);

{ReadMany(databasePlan, partitionPlan)}
{CreateOrReplace(databasePlan, partitionPlan)}
{string.Concat(partitionPlan.Documents.Select(x => Create(partitionPlan, x)))}
{string.Concat(partitionPlan.Documents.Select(x => CreateOrReplace(partitionPlan, x)))}
{string.Concat(partitionPlan.Documents.Select(x => ReadOrCreate(partitionPlan, x)))}
{string.Concat(partitionPlan.Documents.Select(ReplaceIfMutable))}
{string.Concat(partitionPlan.Documents.Select(DeleteIfTransient))}
}}
";

        outputModel.Context.AddSource($"partition_{partitionPlan.ClassName}.cs", s);

        BatchWriter.Write(outputModel, databasePlan, partitionPlan);
        CreateWriter.Write(outputModel, databasePlan, partitionPlan);
        ReadOrCreateWriter.Write(outputModel, databasePlan, partitionPlan);
        CreateOrReplaceWriter.Write(outputModel, databasePlan, partitionPlan);
        ReadWriter.Write(outputModel, databasePlan, partitionPlan);
        ReadOrThrowWriter.Write(outputModel, databasePlan, partitionPlan);
        ReadManyWriter.Write(outputModel, databasePlan, partitionPlan);
        QueryBuilderWriter.Write(outputModel, databasePlan, partitionPlan);
        QueryWriter.Write(outputModel, databasePlan, partitionPlan);
    }

    static string ConstructorClassParameter(DatabasePlan databasePlan) => $"{databasePlan.Namespace}.{databasePlan.DbClassName} {databasePlan.DbClassNameArgument}";

    static string ConstructorKeyParameter(PartitionPlan partitionPlan) =>
        partitionPlan.GetPkPlan.Arguments.Count == 0 ?
        "" : $@"
        in PkData pkData";

    static string PkClass(PartitionPlan partitionPlan) =>
        partitionPlan.GetPkPlan.Arguments.Count == 0
        ? "" :
        $@"
    internal protected struct PkData
    {{
        {string.Concat(partitionPlan.GetPkPlan.Arguments.Select(PkClassProperty))}
    }}

    internal protected virtual PkData PartitionKeyData {{ get; }}
";

    static string PkClassProperty(GetPkIdPlan.Argument argument) => $@"
        public {argument.FullTypeName} {argument.PropertyName} {{ get; init; }}";

    static string PkClassSetter(PartitionPlan partitionPlan) => partitionPlan.GetPkPlan.Arguments.Count == 0 ? "" : "this.PartitionKeyData = pkData;";

    static string ReadMany(DatabasePlan databasePlan, PartitionPlan partitionPlan) =>
        !partitionPlan.Documents.Any(x => x.GetIdPlan.Arguments.Count > 0)
        ? ""
        : $@"
        {partitionPlan.ReadManyClassName}? readMany;
    /// <summary>
    /// Methods to read multiple documents at once, though not necessarily in a single operation.
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.ReadManyClassName} ReadMany => this.readMany ??= new(
        {databasePlan.DbClassNameArgument}: {databasePlan.DbClassName}, 
        partitionKey: this.PartitionKey);
";

    static string CreateOrReplace(DatabasePlan databasePlan, PartitionPlan partitionPlan) =>
        !partitionPlan.Documents.Any(x => x.IsTransient || x.IsMutable)
        ? ""
        : $@"
        {partitionPlan.CreateOrReplaceClassName}? createOrReplace;
    /// <summary>
    /// Methods to create or replace (unconditionally overwrite) documents.
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.CreateOrReplaceClassName} CreateOrReplace => this.createOrReplace ??= new(this);
";

    static string ReplaceIfMutable(DocumentPlan documentPlan) =>
        !documentPlan.IsMutable
        ? ""
        : $@"
    /// <summary>
    /// Try to replace an existing {documentPlan.ClassName}.
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    public virtual System.Threading.Tasks.Task<Cosmogenesis.Core.ReplaceResult<{documentPlan.FullTypeName}>> ReplaceAsync({documentPlan.FullTypeName} {documentPlan.ClassNameArgument})
    {{
        System.ArgumentNullException.ThrowIfNull({documentPlan.ClassNameArgument});
        return this.ReplaceItemAsync(
            item: {documentPlan.ClassNameArgument},
            type: {documentPlan.ConstDocType},
            allowTtl: {(documentPlan.AutoExpires ? "true" : "false")});
    }}
";

    static string DeleteIfTransient(DocumentPlan documentPlan) =>
        !documentPlan.IsTransient
        ? ""
        : $@"
    /// <summary>
    /// Try to delete an existing {documentPlan.ClassName}.
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    public virtual System.Threading.Tasks.Task<Cosmogenesis.Core.DbConflictType?> DeleteAsync({documentPlan.FullTypeName} {documentPlan.ClassNameArgument})
    {{
        System.ArgumentNullException.ThrowIfNull({documentPlan.ClassNameArgument});
        return this.DeleteItemAsync(item: {documentPlan.ClassNameArgument});
    }}
";

    static string Create(PartitionPlan partitionPlan, DocumentPlan documentPlan) => $@"
    /// <summary>
    /// Try to create a {documentPlan.ClassName}.
    /// .id must be set if there is no stable id generator defined
    /// .pk, .CreationDate and .Type are set automatically
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    internal protected virtual System.Threading.Tasks.Task<Cosmogenesis.Core.CreateResult<{documentPlan.FullTypeName}>> CreateAsync({documentPlan.FullTypeName} {documentPlan.ClassNameArgument})
    {{
        {DocumentModelWriter.CreateAndCheckPkAndId(partitionPlan, documentPlan, documentPlan.ClassNameArgument)}
        return this.CreateItemAsync(item: {documentPlan.ClassNameArgument}, type: {documentPlan.ConstDocType});
    }}
";

    static string CreateOrReplace(PartitionPlan partitionPlan, DocumentPlan documentPlan) =>
        !documentPlan.IsMutable && !documentPlan.IsTransient
        ? ""
        : $@"
    /// <summary>
    /// Create or replace (unconditionally overwrite) a {documentPlan.ClassName}.
    /// .id must be set if there is no stable id generator defined
    /// .pk, .CreationDate and .Type are set automatically
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    internal protected virtual System.Threading.Tasks.Task<Cosmogenesis.Core.CreateOrReplaceResult<{documentPlan.FullTypeName}>> CreateOrReplaceAsync({documentPlan.FullTypeName} {documentPlan.ClassNameArgument})
    {{
        {DocumentModelWriter.CreateAndCheckPkAndId(partitionPlan, documentPlan, documentPlan.ClassNameArgument)}
        return this.CreateOrReplaceItemAsync(item: {documentPlan.ClassNameArgument}, type: {documentPlan.ConstDocType}, allowTtl: {(documentPlan.AutoExpires ? "true" : "false")});
    }}
";

    static string ReadOrCreate(PartitionPlan partitionPlan, DocumentPlan documentPlan) => $@"
    /// <summary>
    /// Read a {documentPlan.ClassName} document, or create it if it does not yet exist.
    /// .id must be set if there is no stable id generator defined
    /// .pk, .CreationDate and .Type are set automatically
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    internal protected virtual System.Threading.Tasks.Task<Cosmogenesis.Core.ReadOrCreateResult<{documentPlan.FullTypeName}>> ReadOrCreateAsync(bool tryCreateFirst, {documentPlan.FullTypeName} {documentPlan.ClassNameArgument})
    {{
        {DocumentModelWriter.CreateAndCheckPkAndId(partitionPlan, documentPlan, documentPlan.ClassNameArgument)}
        return this.ReadOrCreateItemAsync(item: {documentPlan.ClassNameArgument}, type: {documentPlan.ConstDocType}, tryCreateFirst: tryCreateFirst);
    }}
";
}

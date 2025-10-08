using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers.Partition;
static class BatchWriter
{
    public static void Write(OutputModel outputModel, DatabasePlan databasePlan, PartitionPlan partitionPlan)
    {
        Write(outputModel, databasePlan, partitionPlan, true);
        Write(outputModel, databasePlan, partitionPlan, false);
    }
    static void Write(OutputModel outputModel, DatabasePlan databasePlan, PartitionPlan partitionPlan, bool withResults)
    {
        var batchClassName = withResults ? partitionPlan.BatchWithResultsClassName : partitionPlan.BatchClassName;
        var s = $@"
namespace {databasePlan.Namespace};

public class {batchClassName} : Cosmogenesis.Core.DbBatchBase
{{
    protected virtual {databasePlan.Namespace}.{partitionPlan.ClassName} {partitionPlan.ClassName} {{ get; }} = default!;

    /// <summary>Mocking constructor</summary>
    protected {batchClassName}() {{ }}

    internal protected {batchClassName}(
        Microsoft.Azure.Cosmos.TransactionalBatch transactionalBatch,
        string partitionKey,
        bool validateStateBeforeSave,
        {databasePlan.Namespace}.{partitionPlan.ClassName} {partitionPlan.ClassNameArgument})
        : base(
            transactionalBatch: transactionalBatch,
            partitionKey: partitionKey,
            serializer: {databasePlan.Namespace}.{databasePlan.SerializerClassName}.Instance,
            validateStateBeforeSave: validateStateBeforeSave,
            withResults: {(withResults ? "true" : "false")})
    {{
        System.ArgumentNullException.ThrowIfNull({partitionPlan.ClassNameArgument});
        this.{partitionPlan.ClassName} = {partitionPlan.ClassNameArgument};
    }}

    /// <summary>
    /// Queue a document for creation in the batch.
    /// Throws InvalidOperationException if the DbDoc does not belong in the partition.
    /// </summary>
    public virtual {databasePlan.Namespace}.{batchClassName} CheckAndCreate(Cosmogenesis.Core.DbDoc dbDoc) => dbDoc switch
    {{
{string.Concat(partitionPlan.Documents.Select(CheckedCreate))}
        null => throw new System.ArgumentNullException(nameof(dbDoc)),
        _ => throw new System.InvalidOperationException($""{{dbDoc.GetType().Name}} is not a type stored in this partition"")
    }};

    /// <summary>
    /// Tries to queue a document for creation in the batch.
    /// Returns true if queued, or false if the document does not belong in the partition.
    /// </summary>
    public virtual bool TryCheckAndCreate(Cosmogenesis.Core.DbDoc dbDoc) => dbDoc switch
    {{
{string.Concat(partitionPlan.Documents.Select(CheckedCreate))}
        null => throw new System.ArgumentNullException(nameof(dbDoc)),
        _ => ({databasePlan.Namespace}.{batchClassName}?)null
    }} != null;

    /// <summary>
    /// Queue a document for creation or replacement in the batch.
    /// Throws InvalidOperationException if the DbDoc does not belong in the partition or is not mutable.
    /// </summary>
    public virtual {databasePlan.Namespace}.{batchClassName} CheckAndCreateOrReplace(Cosmogenesis.Core.DbDoc dbDoc) => dbDoc switch
    {{
{string.Concat(partitionPlan.Documents.Where(x => x.IsMutable || x.IsTransient).Select(CheckedCreateOrReplace))}
        null => throw new System.ArgumentNullException(nameof(dbDoc)),
        _ => throw new System.InvalidOperationException($""{{dbDoc.GetType().Name}} is not a mutable type in this partition"")
    }};

    /// <summary>
    /// Tries to queue a document for creation or replacement in the batch.
    /// Returns true if queued, or false if the document does not belong in the partition or is not mutable.
    /// </summary>
    public virtual bool TryCheckAndCreateOrReplace(Cosmogenesis.Core.DbDoc dbDoc) => dbDoc switch
    {{
{string.Concat(partitionPlan.Documents.Where(x => x.IsMutable || x.IsTransient).Select(CheckedCreateOrReplace))}
        null => throw new System.ArgumentNullException(nameof(dbDoc)),
        _ => ({databasePlan.Namespace}.{batchClassName}?)null
    }} != null;

    /// <summary>
    /// Queue a document for replacement in the batch.
    /// Throws InvalidOperationException if the DbDoc does not belong in the partition or is not mutable.
    /// </summary>
    public virtual {databasePlan.Namespace}.{batchClassName} CheckAndReplace(Cosmogenesis.Core.DbDoc dbDoc) => dbDoc switch
    {{
        {string.Concat(partitionPlan.Documents.Where(x => x.IsMutable).Select(CheckedReplace))}
        null => throw new System.ArgumentNullException(nameof(dbDoc)),
        _ => throw new System.InvalidOperationException($""{{dbDoc.GetType().Name}} is not a mutable type in this partition"")
    }};

    /// <summary>
    /// Tries to queue a document for replacement in the batch.
    /// Returns true if queued, or false if the document does not belong in the partition or is not mutable.
    /// </summary>
    public virtual bool TryCheckAndReplace(Cosmogenesis.Core.DbDoc dbDoc) => dbDoc switch
    {{
        {string.Concat(partitionPlan.Documents.Where(x => x.IsMutable).Select(CheckedReplace))}
        null => throw new System.ArgumentNullException(nameof(dbDoc)),
        _ => ({databasePlan.Namespace}.{batchClassName}?)null
    }} != null;

    /// <summary>
    /// Queue a document for deletion in the batch.
    /// Throws InvalidOperationException if the DbDoc does not belong in the partition or is not transient.
    /// </summary>
    public virtual {databasePlan.Namespace}.{batchClassName} CheckAndDelete(Cosmogenesis.Core.DbDoc dbDoc) => dbDoc switch
    {{
        {string.Concat(partitionPlan.Documents.Where(x => x.IsTransient).Select(CheckedDelete))}
        null => throw new System.ArgumentNullException(nameof(dbDoc)),
        _ => throw new System.InvalidOperationException($""{{dbDoc.GetType().Name}} is not a transient type in this partition"")
    }};

    /// <summary>
    /// Tries to queue a document for deletion in the batch.
    /// Returns true if queued, or false if the document does not belong in the partition or is not transient.
    /// </summary>
    public virtual bool TryCheckAndDelete(Cosmogenesis.Core.DbDoc dbDoc) => dbDoc switch
    {{
        {string.Concat(partitionPlan.Documents.Where(x => x.IsTransient).Select(CheckedDelete))}
        null => throw new System.ArgumentNullException(nameof(dbDoc)),
        _ => ({databasePlan.Namespace}.{batchClassName}?)null
    }} != null;

{string.Concat(partitionPlan.Documents.Select(x => Create(databasePlan, partitionPlan, x, batchClassName)))}
{string.Concat(partitionPlan.Documents.Select(x => CreateOrReplace(databasePlan, partitionPlan, x, batchClassName)))}
{string.Concat(partitionPlan.Documents.Select(x => Replace(databasePlan, partitionPlan, x, batchClassName)))}
{string.Concat(partitionPlan.Documents.Select(x => Delete(databasePlan, partitionPlan, x, batchClassName)))}
{(withResults ? ExecuteWithResults() : Execute())}
}}
";

            outputModel.Context.AddSource($"partition_{batchClassName}.cs", s);
    }

    static string Execute() => $@"
    /// <summary>
    /// Atomicly executes all operations in this batch.
    /// Returns true if all operations succeeded.
    /// Returns false if any operation failed (no changes made).
    /// A batch can be submitted only once for execution.
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    /// <exception cref=""System.InvalidOperationException"" />
    protected virtual Task<bool> ExecuteAsync() => base.ExecuteCoreAsync();


    /// <summary>
    /// Atomicly executes all operations in this batch.
    /// Throws an exception on failure.
    /// A batch can be submitted only once for execution.
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    /// <exception cref=""Cosmogenesis.Core.DbConflictException"" />
    /// <exception cref=""System.InvalidOperationException"" />
    public virtual Task ExecuteOrThrowAsync() => base.ExecuteOrThrowCoreAsync();
";

    static string ExecuteWithResults() => $@"
    /// <summary>
    /// Atomicly executes all operations in this batch.
    /// Returns true if all operations succeeded.
    /// Returns false if any operation failed (no changes made).
    /// A batch can be submitted only once for execution.
    /// </summary>
    /// <exception cref=""Cosmogenesis.Core.DbOverloadedException"" />
    /// <exception cref=""Cosmogenesis.Core.DbUnknownStatusCodeException"" />
    /// <exception cref=""System.InvalidOperationException"" />
    public virtual Task<Cosmogenesis.Core.BatchResult> ExecuteAsync() => base.ExecuteWithResultsCoreAsync();
";

    static string CheckedCreateOrReplace(DocumentPlan documentPlan) => $@"
        {documentPlan.FullTypeName} x => this.CreateOrReplace({documentPlan.ClassNameArgument}: x),";

    static string CheckedCreate(DocumentPlan documentPlan) => $@"
        {documentPlan.FullTypeName} x => this.Create({documentPlan.ClassNameArgument}: x),";

    static string CheckedReplace(DocumentPlan documentPlan) => $@"
        {documentPlan.FullTypeName} x => this.Replace({documentPlan.ClassNameArgument}: x),";

    static string CheckedDelete(DocumentPlan documentPlan) => $@"
        {documentPlan.FullTypeName} x => this.Delete({documentPlan.ClassNameArgument}: x),";

    static string Create(DatabasePlan databasePlan, PartitionPlan partitionPlan, DocumentPlan documentPlan, string batchClassName) => $@"
    /// <summary>
    /// Queue a {documentPlan.ClassName} for creation in the batch
    /// </summary>
    protected virtual {databasePlan.Namespace}.{batchClassName} Create({documentPlan.FullTypeName} {documentPlan.ClassNameArgument})
    {{
        {DocumentModelWriter.CreateAndCheckPkAndId(partitionPlan, documentPlan, documentPlan.ClassNameArgument)}
        this.CreateCore(item: {documentPlan.ClassNameArgument}, type: {documentPlan.ConstDocType});
        return this;
    }}

    /// <summary>
    /// Queue a {documentPlan.ClassName} for creation in the batch
    /// </summary>
    public virtual {databasePlan.Namespace}.{batchClassName} Create{documentPlan.ClassName}({documentPlan.PropertiesByName.Values.Where(x => !partitionPlan.GetPkPlan.ArgumentByPropertyName.ContainsKey(x.PropertyName)).AsInputParameters(documentPlan)}) =>
        this.Create({documentPlan.ClassNameArgument}: new {documentPlan.FullTypeName} {{ {partitionPlan.AsSettersFromDocumentPlanAndPartitionClass(documentPlan)} }});
";

    static string CreateOrReplace(DatabasePlan databasePlan, PartitionPlan partitionPlan, DocumentPlan documentPlan, string batchClassName) =>
        !documentPlan.IsTransient && !documentPlan.IsMutable
        ? ""
        : $@"
    /// <summary>
    /// Queue a {documentPlan.ClassName} for creation or replacement in the batch
    /// </summary>
    protected virtual {databasePlan.Namespace}.{batchClassName} CreateOrReplace({documentPlan.FullTypeName} {documentPlan.ClassNameArgument})
    {{    
        {DocumentModelWriter.CreateAndCheckPkAndId(partitionPlan, documentPlan, documentPlan.ClassNameArgument)}
        this.CreateOrReplaceCore(item: {documentPlan.ClassNameArgument}, type: {documentPlan.ConstDocType});
        return this;
    }}

    /// <summary>
    /// Queue a {documentPlan.ClassName} for creation or replacement in the batch
    /// </summary>
    public virtual {databasePlan.Namespace}.{batchClassName} CreateOrReplace{documentPlan.ClassName}({documentPlan.PropertiesByName.Values.Where(x => !partitionPlan.GetPkPlan.ArgumentByPropertyName.ContainsKey(x.PropertyName)).AsInputParameters(documentPlan)}) =>
        this.CreateOrReplace({documentPlan.ClassNameArgument}: new {documentPlan.FullTypeName} {{ {partitionPlan.AsSettersFromDocumentPlanAndPartitionClass(documentPlan)} }});
";

    static string Replace(DatabasePlan databasePlan, PartitionPlan partitionPlan, DocumentPlan documentPlan, string batchClassName) =>
        !documentPlan.IsMutable
        ? ""
        : $@"
    /// <summary>
    /// Queue a {documentPlan.ClassName} for replacement in the batch
    /// </summary>
    public virtual {databasePlan.Namespace}.{batchClassName} Replace({documentPlan.FullTypeName} {documentPlan.ClassNameArgument})
    {{    
        this.ReplaceCore(item: {documentPlan.ClassNameArgument}, type: {documentPlan.ConstDocType});
        return this;
    }}
";

    static string Delete(DatabasePlan databasePlan, PartitionPlan partitionPlan, DocumentPlan documentPlan, string batchClassName) =>
        !documentPlan.IsTransient
        ? ""
        : $@"
    /// <summary>
    /// Queue a {documentPlan.ClassName} for deletion in the batch
    /// </summary>
    public virtual {databasePlan.Namespace}.{batchClassName} Delete({documentPlan.FullTypeName} {documentPlan.ClassNameArgument})
    {{
        this.DeleteCore(item: {documentPlan.ClassNameArgument});
        return this;
    }}
";
}

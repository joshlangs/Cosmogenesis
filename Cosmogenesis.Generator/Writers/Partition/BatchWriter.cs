using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers.Partition;
static class BatchWriter
{
    public static void Write(OutputModel outputModel, DatabasePlan databasePlan, PartitionPlan partitionPlan)
    {
        var s = $@"
namespace {databasePlan.Namespace};

public class {partitionPlan.BatchClassName} : Cosmogenesis.Core.DbBatchBase
{{
    protected virtual {databasePlan.Namespace}.{partitionPlan.ClassName} {partitionPlan.ClassName} {{ get; }} = default!;

    /// <summary>Mocking constructor</summary>
    protected {partitionPlan.BatchClassName}() {{ }}

    internal protected {partitionPlan.BatchClassName}(
        Microsoft.Azure.Cosmos.TransactionalBatch transactionalBatch,
        string partitionKey,
        bool validateStateBeforeSave,
        {databasePlan.Namespace}.{partitionPlan.ClassName} {partitionPlan.ClassNameArgument})
        : base(
            transactionalBatch: transactionalBatch,
            partitionKey: partitionKey,
            serializer: {databasePlan.Namespace}.{databasePlan.SerializerClassName}.Instance,
            validateStateBeforeSave: validateStateBeforeSave)
    {{
        this.{partitionPlan.ClassName} = {partitionPlan.ClassNameArgument} ?? throw new System.ArgumentNullException(nameof({partitionPlan.ClassNameArgument}));
    }}

    /// <summary>
    /// Queue a document for creation in the batch.
    /// Throws InvalidOperationException if the DbDoc does not belong in the partition.
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.BatchClassName} CheckAndCreate(Cosmogenesis.Core.DbDoc dbDoc) => dbDoc switch
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
        _ => ({databasePlan.Namespace}.{partitionPlan.BatchClassName}?)null
    }} != null;

    /// <summary>
    /// Queue a document for creation or replacement in the batch.
    /// Throws InvalidOperationException if the DbDoc does not belong in the partition or is not mutable.
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.BatchClassName} CheckAndCreateOrReplace(Cosmogenesis.Core.DbDoc dbDoc) => dbDoc switch
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
        _ => ({databasePlan.Namespace}.{partitionPlan.BatchClassName}?)null
    }} != null;

    /// <summary>
    /// Queue a document for replacement in the batch.
    /// Throws InvalidOperationException if the DbDoc does not belong in the partition or is not mutable.
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.BatchClassName} CheckAndReplace(Cosmogenesis.Core.DbDoc dbDoc) => dbDoc switch
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
        _ => ({databasePlan.Namespace}.{partitionPlan.BatchClassName}?)null
    }} != null;

    /// <summary>
    /// Queue a document for deletion in the batch.
    /// Throws InvalidOperationException if the DbDoc does not belong in the partition or is not transient.
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.BatchClassName} CheckAndDelete(Cosmogenesis.Core.DbDoc dbDoc) => dbDoc switch
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
        _ => ({databasePlan.Namespace}.{partitionPlan.BatchClassName}?)null
    }} != null;

{string.Concat(partitionPlan.Documents.Select(x => Create(databasePlan, partitionPlan, x)))}
{string.Concat(partitionPlan.Documents.Select(x => CreateOrReplace(databasePlan, partitionPlan, x)))}
{string.Concat(partitionPlan.Documents.Select(x => Replace(databasePlan, partitionPlan, x)))}
{string.Concat(partitionPlan.Documents.Select(x => Delete(databasePlan, partitionPlan, x)))}
}}
";
        outputModel.Context.AddSource($"partition_{partitionPlan.BatchClassName}.cs", s);
    }

    static string CheckedCreateOrReplace(DocumentPlan documentPlan) => $@"
        {documentPlan.FullTypeName} x => this.CreateOrReplace({documentPlan.ClassNameArgument}: x),";

    static string CheckedCreate(DocumentPlan documentPlan) => $@"
        {documentPlan.FullTypeName} x => this.Create({documentPlan.ClassNameArgument}: x),";

    static string CheckedReplace(DocumentPlan documentPlan) => $@"
        {documentPlan.FullTypeName} x => this.Replace({documentPlan.ClassNameArgument}: x),";

    static string CheckedDelete(DocumentPlan documentPlan) => $@"
        {documentPlan.FullTypeName} x => this.Delete({documentPlan.ClassNameArgument}: x),";

    static string Create(DatabasePlan databasePlan, PartitionPlan partitionPlan, DocumentPlan documentPlan) => $@"
    /// <summary>
    /// Queue a {documentPlan.ClassName} for creation in the batch
    /// </summary>
    protected virtual {databasePlan.Namespace}.{partitionPlan.BatchClassName} Create({documentPlan.FullTypeName} {documentPlan.ClassNameArgument})
    {{
        {DocumentModelWriter.CreateAndCheckPkAndId(partitionPlan, documentPlan, documentPlan.ClassNameArgument)}
        this.CreateCore(item: {documentPlan.ClassNameArgument}, type: {documentPlan.ConstDocType});
        return this;
    }}

    /// <summary>
    /// Queue a {documentPlan.ClassName} for creation in the batch
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.BatchClassName} Create{documentPlan.ClassName}({documentPlan.PropertiesByName.Values.Where(x => !partitionPlan.GetPkPlan.ArgumentByPropertyName.ContainsKey(x.PropertyName)).AsInputParameters(documentPlan)}) =>
        this.Create({documentPlan.ClassNameArgument}: new {documentPlan.FullTypeName} {{ {partitionPlan.AsSettersFromDocumentPlanAndPartitionClass(documentPlan)} }});
";

    static string CreateOrReplace(DatabasePlan databasePlan, PartitionPlan partitionPlan, DocumentPlan documentPlan) =>
        !documentPlan.IsTransient && !documentPlan.IsMutable
        ? ""
        : $@"
    /// <summary>
    /// Queue a {documentPlan.ClassName} for creation or replacement in the batch
    /// </summary>
    protected virtual {databasePlan.Namespace}.{partitionPlan.BatchClassName} CreateOrReplace({documentPlan.FullTypeName} {documentPlan.ClassNameArgument})
    {{    
        {DocumentModelWriter.CreateAndCheckPkAndId(partitionPlan, documentPlan, documentPlan.ClassNameArgument)}
        this.CreateOrReplaceCore(item: {documentPlan.ClassNameArgument}, type: {documentPlan.ConstDocType});
        return this;
    }}

    /// <summary>
    /// Queue a {documentPlan.ClassName} for creation or replacement in the batch
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.BatchClassName} CreateOrReplace{documentPlan.ClassName}({documentPlan.PropertiesByName.Values.Where(x => !partitionPlan.GetPkPlan.ArgumentByPropertyName.ContainsKey(x.PropertyName)).AsInputParameters(documentPlan)}) =>
        this.CreateOrReplace({documentPlan.ClassNameArgument}: new {documentPlan.FullTypeName} {{ {partitionPlan.AsSettersFromDocumentPlanAndPartitionClass(documentPlan)} }});
";

    static string Replace(DatabasePlan databasePlan, PartitionPlan partitionPlan, DocumentPlan documentPlan) =>
        !documentPlan.IsMutable
        ? ""
        : $@"
    /// <summary>
    /// Queue a {documentPlan.ClassName} for replacement in the batch
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.BatchClassName} Replace({documentPlan.FullTypeName} {documentPlan.ClassNameArgument})
    {{    
        this.ReplaceCore(item: {documentPlan.ClassNameArgument}, type: {documentPlan.ConstDocType});
        return this;
    }}
";

    static string Delete(DatabasePlan databasePlan, PartitionPlan partitionPlan, DocumentPlan documentPlan) =>
        !documentPlan.IsTransient
        ? ""
        : $@"
    /// <summary>
    /// Queue a {documentPlan.ClassName} for deletion in the batch
    /// </summary>
    public virtual {databasePlan.Namespace}.{partitionPlan.BatchClassName} Delete({documentPlan.FullTypeName} {documentPlan.ClassNameArgument})
    {{
        this.DeleteCore(item: {documentPlan.ClassNameArgument});
        return this;
    }}
";
}

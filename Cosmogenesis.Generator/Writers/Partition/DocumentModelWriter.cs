using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers.Partition;
static class DocumentModelWriter
{
    public static string CreateAndCheckPkAndId(PartitionPlan partitionPlan, DocumentPlan documentPlan, string paramTypeName) => $@"
        System.ArgumentNullException.ThrowIfNull({paramTypeName});
        var calculatedPk = {partitionPlan.GetPkPlan.FullMethodName}({partitionPlan.GetPkPlan.DocumentToParametersMapping(paramTypeName)});
        var calculatedId = Cosmogenesis.Core.DbDocHelper.GetValidId({documentPlan.GetIdPlan.FullMethodName}({documentPlan.GetIdPlan.DocumentToParametersMapping(paramTypeName)}));
        if ({paramTypeName}.id is null)
        {{
            {paramTypeName}.id = calculatedId ?? throw new System.InvalidOperationException(""The generated document id cannot be null"");
        }}
        else if ({paramTypeName}.id != calculatedId) 
        {{
            throw new System.InvalidOperationException(""The document .id property does not match the calculated document id"");
        }}
        if ({paramTypeName}.pk is null)
        {{
            {paramTypeName}.pk = calculatedPk ?? throw new System.InvalidOperationException(""The generated partition key cannot be null"");
        }}
        else if ({paramTypeName}.pk != calculatedPk)
        {{
            throw new System.InvalidOperationException(""The document .pk property does not match the calculated document partition key"");
        }}
";
}

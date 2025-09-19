using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers;
static class ConverterWriter
{
    public static void Write(OutputModel outputModel,  DatabasePlan databasePlan)
    {
        var s = $@"
namespace {databasePlan.Namespace};

/// <summary>
/// This converter knows how to convert the DbDoc base type into documents from the {databasePlan.Name} database.
/// </summary>
public class {databasePlan.ConverterClassName} : Cosmogenesis.Core.DbDocConverterBase
{{
    /// <summary>
    /// This converter knows how to convert the DbDoc base type into documents from the {databasePlan.Name} database.
    /// </summary>
    public static readonly {databasePlan.Namespace}.{databasePlan.ConverterClassName} Instance = new();

    protected override Cosmogenesis.Core.DbDoc? DeserializeByType(
        ref System.Text.Json.Utf8JsonReader reader, 
        string? type, 
        System.Text.Json.JsonSerializerOptions options) => type switch
        {{
{string.Concat(databasePlan.PartitionPlansByName.Values.SelectMany(x => x.Documents).Select(DeserializeType))}
            _ => throw new System.NotSupportedException($""We don't know how to deserialize a message of type {{type}}"")
        }};
}}
";

        outputModel.Context.AddSource($"db_{databasePlan.ConverterClassName}.cs", s);
    }
    static string DeserializeType(DocumentPlan documentPlan) => $@"
            {documentPlan.ConstDocType} => System.Text.Json.JsonSerializer.Deserialize<{documentPlan.FullTypeName}>(reader: ref reader, options: options),";
}

using Cosmogenesis.Generator.Models;

namespace Cosmogenesis.Generator.Plans;
class DocumentPlan
{
    public string DocType = default!;
    public string ClassName = default!;
    public string ClassNameArgument = default!;
    public string PluralName = default!;
    public GetPkIdPlan GetIdPlan = default!;
    public ClassModel ClassModel = default!;
    public string FullTypeName = default!;
    public bool IsMutable;
    public bool IsTransient;
    public bool AutoExpires;
    public int? DefaultTtl;
    public readonly Dictionary<string, PropertyPlan> PropertiesByName = new();
    public readonly Dictionary<string, PropertyPlan> PropertiesByArgumentName = new();
    public string ConstDocType = default!;
}

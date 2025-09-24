using Cosmogenesis.Generator.Models;

namespace Cosmogenesis.Generator.Plans;
class PartitionPlan
{
    public string Name = default!;
    public string ClassName = default!;
    public string ClassNameArgument = default!;
    public string PluralName = default!;
    public MethodModel GetPkModel = default!;
    public GetPkIdPlan GetPkPlan = default!;
    public readonly List<DocumentPlan> Documents = [];
    public readonly List<UnionPlan> Unions = [];
    public string BatchHandlersClassName = default!;
    public string BatchHandlersClassNameArgument = default!;
    public string CreateOrReplaceClassName = default!;
    public string ReadClassName = default!;
    public string ReadUnionsClassName = default!;
    public string ReadOrThrowClassName = default!;
    public string ReadOrThrowUnionsClassName = default!;
    public string ReadManyClassName = default!;
    public string ReadManyUnionsClassName = default!;
    public string QueryBuilderClassName = default!;
    public string QueryBuilderUnionsClassName = default!;
    public string QueryClassName = default!;
    public string QueryUnionsClassName = default!;
    public string QueryBuilderClassNameArgument = default!;
    public string ReadOrCreateClassName = default!;
    public string CreateClassName = default!;
    public string BatchClassName = default!;
}

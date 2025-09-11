using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;
using Microsoft.CodeAnalysis;

namespace Cosmogenesis.Generator.PlanBuilders;
static class GetPkIdPlanBuilder
{
    public static GetPkIdPlan Build(OutputModel outputModel, IMethodSymbol symbol, params Dictionary<string, PropertyPlan>[] propertyPlanByNames)
    {
        var plan = new GetPkIdPlan()
        {
            FullMethodName = $"{symbol.ContainingType.ToDisplayString()}.{symbol.Name}",
            Arguments = [.. symbol
                .Parameters
                .Select(x =>
                {
                    var arg = new GetPkIdPlan.Argument
                    {
                        ArgumentName = x.Name,
                        FullTypeName = x.Type.ToDisplayString()
                    };
                    var names = new List<string>(4) { arg.ArgumentName };
                    if (arg.ArgumentName.StartsWith("_"))
                    {
                        names.Add(arg.ArgumentName.Substring(1));
                        names.Add(arg.ArgumentName.Substring(1).ToPascalCase());
                    }
                    else
                    {
                        names.Add(arg.ArgumentName.ToPascalCase());
                    }
                    foreach (var name in names)
                    {
                        foreach (var dict in propertyPlanByNames)
                        {
                            if (dict.TryGetValue(name, out var propertyPlan) && propertyPlan.FullTypeName == arg.FullTypeName)
                            {
                                arg.PropertyName = propertyPlan.PropertyName;
                                if (!propertyPlan.IsInitOnly)
                                {
                                    outputModel.Report(Diagnostics.Warnings.InitOnlyKey, propertyPlan.PropertyModel.PropertySymbol, arg.PropertyName);
                                }
                                return arg;
                            }
                        }
                    }
                    outputModel.Report(Diagnostics.Errors.PropertyResolvePkId, symbol, arg.ArgumentName);
                    return arg;
                })]
        };
        plan.ArgumentByPropertyName = plan.Arguments.Where(x => x.PropertyName is not null).ToDictionary(x => x.PropertyName);
        return plan;
    }
}

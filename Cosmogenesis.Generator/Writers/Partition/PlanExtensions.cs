using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.Writers.Partition;
static class PlanExtensions
{
    public static string DocumentToParametersMapping(this GetPkIdPlan plan, string parameterName) =>
        plan.Arguments.Select(x => $"{x.ArgumentName}: {parameterName}.{x.PropertyName}").JoinNonEmpty();

    public static string AsInputParameters(this GetPkIdPlan plan) =>
        plan.Arguments.Select(x => $"{x.FullTypeName} {x.ArgumentName}").JoinNonEmpty();

    public static string AsInputParameters(this IEnumerable<PropertyPlan> properties, DocumentPlan documentPlan)
    {
        var args = properties
            .OrderBy(x => x.UseDefault)
            .Select(x => $"{x.FullTypeName} {x.ArgumentName}{(x.UseDefault ? " = default" : "")}");
        if (documentPlan.AutoExpires)
        {
            args = args.Concat([$"int? ttl = {(documentPlan.DefaultTtl.HasValue ? documentPlan.DefaultTtl.Value.ToString() : "default")}"]);
        }
        return args.JoinNonEmpty();
    }

    public static string AsInputParameterMapping(this IEnumerable<PropertyPlan> properties) =>
        properties.Select(x => $"{x.ArgumentName}: {x.ArgumentName}").JoinNonEmpty();

    public static string AsInputParameterMapping(this GetPkIdPlan plan) =>
        plan.Arguments.Select(x => $"{x.ArgumentName}: {x.ArgumentName}").JoinNonEmpty();

    public static string AsSettersFromParameters(this IEnumerable<PropertyPlan> properties) =>
        properties.Select(x => $"{x.PropertyName} = {x.ArgumentName}").JoinNonEmpty();

    public static string AsSettersFromParameters(this GetPkIdPlan properties) =>
        properties.Arguments.Select(x => $"{x.PropertyName} = {x.ArgumentName}").JoinNonEmpty();

    public static string ParametersToParametersMapping(this GetPkIdPlan plan, string parameterName) =>
        plan.Arguments.Select(x => $"{x.ArgumentName}: {parameterName}.{x.ArgumentName}").JoinNonEmpty();

    public static string AsSettersFromDocumentPlanAndPartitionClass(this PartitionPlan partitionPlan, DocumentPlan documentPlan)
    {
        var setters = documentPlan
            .PropertiesByName
            .Values
            .Where(x => !partitionPlan.GetPkPlan.ArgumentByPropertyName.ContainsKey(x.PropertyName))
            .Select(x => $"{x.PropertyName} = {x.ArgumentName}");
        if (documentPlan.AutoExpires)
        {
            setters = setters.Concat([$"ttl = ttl"]);
        }
        var key = partitionPlan
            .GetPkPlan
            .Arguments
            .Select(x => $"{x.PropertyName} = this.{partitionPlan.ClassName}.PartitionKeyData.{x.PropertyName}");
        return setters.Concat(key).JoinNonEmpty();
    }
}

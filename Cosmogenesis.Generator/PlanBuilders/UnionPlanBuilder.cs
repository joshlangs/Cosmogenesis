using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;

namespace Cosmogenesis.Generator.PlanBuilders;
class UnionPlanBuilder
{
    public static void AddUnions(OutputModel outputModel, OutputPlan outputPlan)
    {
        foreach (var databasePlan in outputPlan.DatabasePlansByName.Values)
        {
            foreach (var partitionPlan in databasePlan.PartitionPlansByName.Values)
            {
                foreach (var union in partitionPlan.Documents.GroupBy(x => x.GetIdPlan.FullMethodName).Where(x => x.Count() > 1))
                {
                    var idPlan = union.First().GetIdPlan;
                    var fullTypeName = idPlan.FullMethodName.Substring(0, idPlan.FullMethodName.LastIndexOf('.'));
                    var commonName = fullTypeName.Split('.').Last();
                    if (commonName.EndsWith("Base"))
                    {
                        commonName = commonName.Substring(0, commonName.Length - 4);
                    }
                    if (commonName.EndsWith("Doc"))
                    {
                        commonName = commonName.Substring(0, commonName.Length - 3);
                    }
                    partitionPlan.Unions.Add(new UnionPlan
                    {
                        GetIdPlan = idPlan,
                        Documents = [.. union],
                        FullCommonTypeName = fullTypeName,
                        CommonName = commonName
                    });
                }
            }
        }
    }
}

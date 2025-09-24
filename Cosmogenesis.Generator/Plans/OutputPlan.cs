using Cosmogenesis.Generator.Models;

namespace Cosmogenesis.Generator.Plans;
class OutputPlan
{
    public readonly Dictionary<string, DatabasePlan> DatabasePlansByName = [];
    public readonly Dictionary<ClassModel, List<DatabasePlan>> DatabasePlansByClass = [];
}

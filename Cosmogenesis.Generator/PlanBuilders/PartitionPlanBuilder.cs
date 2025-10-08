using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;
using Microsoft.CodeAnalysis;

namespace Cosmogenesis.Generator.PlanBuilders;
static class PartitionPlanBuilder
{
    public static void AddPartitions(OutputModel outputModel, OutputPlan outputPlan)
    {
        outputModel.CancellationToken.ThrowIfCancellationRequested();
        if (!outputModel.CanGenerate) { return; }

        AddPartitionClasses(outputModel, outputPlan);
        AddImplicitPartitions(outputModel, outputPlan);
    }

    public static void RemoveEmptyPartitions(OutputModel outputModel, OutputPlan outputPlan)
    {
        outputModel.CancellationToken.ThrowIfCancellationRequested();
        if (!outputModel.CanGenerate) { return; }

        foreach (var databasePlan in outputPlan.DatabasePlansByName.Values)
        {
            foreach (var partitionPlan in databasePlan.PartitionPlansByName.Values.Where(x => x.Documents.Count == 0).ToList())
            {
                databasePlan.PartitionPlansByName.Remove(partitionPlan.Name);
                outputModel.Report(Diagnostics.Warnings.EmptyPartition, partitionPlan.GetPkModel.MethodSymbol, partitionPlan.Name);
            }
        }
    }

    static void AddPartitionClasses(OutputModel outputModel, OutputPlan outputPlan)
    {
        foreach (var kvp in outputPlan.DatabasePlansByClass)
        {
            var classModel = kvp.Key;
            var databasePlans = kvp.Value;
            if (classModel.PartitionDefinitionAttribute is not null)
            {
                if (databasePlans.Count == 0)
                {
                    outputModel.Report(Diagnostics.Errors.NoDatabase, classModel.ClassSymbol);
                }
                foreach (var methodModel in classModel.Methods)
                {
                    var name = methodModel.MethodSymbol.Name;
                    foreach (var databasePlan in databasePlans)
                    {
                        AddPartitionDefinition(outputModel, databasePlan, methodModel, name);
                    }
                }
            }
        }
    }
    static void AddImplicitPartitions(OutputModel outputModel, OutputPlan outputPlan)
    {
        foreach (var kvp in outputPlan.DatabasePlansByClass)
        {
            var classModel = kvp.Key;
            var databasePlans = kvp.Value;
            if (classModel.PartitionAttribute is not null)
            {
                var name = classModel.PartitionAttribute.Name;
                foreach (var databasePlan in databasePlans)
                {
                    if (!databasePlan.PartitionPlansByName.ContainsKey(name))
                    {
                        var getPk = classModel.Methods
                            .Where(x => x.MethodSymbol.Name == "GetPk")
                            .Where(x => x.MethodSymbol.IsStatic)
                            .Where(x => x.MethodSymbol.ReturnType.SpecialType == SpecialType.System_String)
                            .Where(x => x.MethodSymbol.DeclaredAccessibility.IsAccessible())
                            .ToList();
                        if (getPk.Count == 0)
                        {
                            outputModel.Report(Diagnostics.Errors.NoGetPk, classModel.ClassSymbol);
                        }
                        else if (getPk.Count > 1)
                        {
                            outputModel.Report(Diagnostics.Errors.MultipleGetPk, classModel.ClassSymbol);
                        }
                        else
                        {
                            AddPartitionDefinition(outputModel, databasePlan, getPk[0], name);
                        }
                    }
                }
            }
        }
    }
    static void AddPartitionDefinition(OutputModel outputModel, DatabasePlan databasePlan, MethodModel methodModel, string name)
    {
        if (databasePlan.PartitionPlansByName.TryGetValue(name, out var partitionPlan))
        {
            if (!SymbolEqualityComparer.Default.Equals(partitionPlan.GetPkModel.MethodSymbol, methodModel.MethodSymbol))
            {
                outputModel.Report(Diagnostics.Errors.PartitionAlreadyDefined, methodModel.MethodSymbol);
            }
        }
        else
        {
            partitionPlan = new PartitionPlan
            {
                Name = name,
                PluralName = name.Pluralize(),
                ClassName = name.WithSuffix(Suffixes.Partition),
                BatchHandlersClassName = name.WithSuffix(Suffixes.BatchHandlers),
                CreateOrReplaceClassName = name.WithSuffix(Suffixes.CreateOrReplace),
                CreateOrReplaceWithoutResultClassName = name.WithSuffix(Suffixes.CreateOrReplaceWithoutResult),
                ReadClassName = name.WithSuffix(Suffixes.Read),
                ReadOrThrowClassName = name.WithSuffix(Suffixes.ReadOrThrow),
                ReadManyClassName = name.WithSuffix(Suffixes.ReadMany),
                ReadUnionsClassName = name.WithSuffix(Suffixes.ReadUnions),
                ReadOrThrowUnionsClassName = name.WithSuffix(Suffixes.ReadOrThrowUnions),
                ReadManyUnionsClassName = name.WithSuffix(Suffixes.ReadManyUnions),
                QueryBuilderClassName = name.WithSuffix(Suffixes.QueryBuilder),
                QueryBuilderUnionsClassName = name.WithSuffix(Suffixes.QueryBuilderUnions),
                QueryClassName = name.WithSuffix(Suffixes.Query),
                QueryUnionsClassName = name.WithSuffix(Suffixes.QueryUnions),
                ReadOrCreateClassName = name.WithSuffix(Suffixes.ReadOrCreate),
                CreateClassName = name.WithSuffix(Suffixes.Create),
                CreateWithoutResultClassName = name.WithSuffix(Suffixes.CreateWithoutResult),
                BatchClassName = name.WithSuffix(Suffixes.Batch),
                BatchWithResultsClassName = name.WithSuffix(Suffixes.BatchWithResults),
                GetPkModel = methodModel
            };
            partitionPlan.BatchHandlersClassNameArgument = partitionPlan.BatchHandlersClassName.ToArgumentName();
            partitionPlan.ClassNameArgument = partitionPlan.ClassName.ToArgumentName();
            databasePlan.PartitionPlansByName[name] = partitionPlan;
            partitionPlan.QueryBuilderClassNameArgument = partitionPlan.QueryBuilderClassName.ToArgumentName();

            outputModel.ValidateNames(
                methodModel.MethodSymbol,
                partitionPlan.Name,
                partitionPlan.PluralName,
                partitionPlan.ClassName,
                partitionPlan.BatchHandlersClassName,
                partitionPlan.CreateOrReplaceClassName,
                partitionPlan.ReadClassName,
                partitionPlan.ReadOrThrowClassName,
                partitionPlan.ReadManyClassName,
                partitionPlan.QueryBuilderClassName,
                partitionPlan.QueryClassName,
                partitionPlan.ReadOrCreateClassName,
                partitionPlan.CreateClassName,
                partitionPlan.BatchClassName,
                partitionPlan.QueryBuilderUnionsClassName,
                partitionPlan.QueryUnionsClassName,
                partitionPlan.ReadManyUnionsClassName,
                partitionPlan.ReadOrThrowUnionsClassName,
                partitionPlan.ReadUnionsClassName);

            outputModel.ValidateIdentifiers(
                methodModel.MethodSymbol,
                partitionPlan.ClassNameArgument,
                partitionPlan.BatchHandlersClassNameArgument,
                partitionPlan.QueryBuilderClassNameArgument);
        }
    }
}

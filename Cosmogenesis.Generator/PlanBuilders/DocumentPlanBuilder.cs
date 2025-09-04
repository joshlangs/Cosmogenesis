using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Plans;
using Microsoft.CodeAnalysis;

namespace Cosmogenesis.Generator.PlanBuilders;
static class DocumentPlanBuilder
{
    public static void AddDocuments(OutputModel outputModel, OutputPlan outputPlan)
    {
        outputModel.CancellationToken.ThrowIfCancellationRequested();
        if (!outputModel.CanGenerate) { return; }

        foreach (var kvp in outputPlan.DatabasePlansByClass)
        {
            var classModel = kvp.Key;
            var databasePlans = kvp.Value;
            if (classModel.IsDbDoc && !classModel.ClassSymbol.IsAbstract)
            {
                if (databasePlans.Count == 0)
                {
                    outputModel.Report(Diagnostics.Errors.NoDatabase, classModel.ClassSymbol);
                }
                else if (classModel.PartitionAttribute is null)
                {
                    outputModel.Report(Diagnostics.Warnings.DbDocWithoutPartition, classModel.ClassSymbol, classModel.ClassSymbol.Name);
                }
                else
                {
                    if (!classModel.ClassSymbol.Constructors.Any(x => x.Parameters.IsDefaultOrEmpty && x.DeclaredAccessibility.IsAccessible()))
                    {
                        outputModel.Report(Diagnostics.Errors.ParameterlessConstructor, classModel.ClassSymbol);
                    }
                    var implicitGetIds = classModel.Methods
                        .Where(x => x.MethodSymbol.Name == "GetId")
                        .Where(x => x.MethodSymbol.IsStatic)
                        .Where(x => x.MethodSymbol.ReturnType.SpecialType == SpecialType.System_String)
                        .Where(x => x.MethodSymbol.DeclaredAccessibility.IsAccessible())
                        .ToList();
                    if (implicitGetIds.Count == 0)
                    {
                        outputModel.Report(Diagnostics.Errors.NoGetId, classModel.ClassSymbol);
                    }
                    else if (implicitGetIds.Count > 1)
                    {
                        outputModel.Report(Diagnostics.Errors.MultipleGetId, classModel.ClassSymbol);
                    }
                    else
                    {
                        var getId = implicitGetIds[0];
                        var partitionName = classModel.PartitionAttribute.Name;
                        var name = classModel.ClassSymbol.Name.WithoutSuffix(Suffixes.Doc).WithoutSuffix(Suffixes.Document);
                        var type = classModel.DocTypeAttribute?.Name.NullIfEmpty() ?? name;
                        type.ValidateDocType(outputModel, classModel.ClassSymbol);
                        foreach (var databasePlan in databasePlans)
                        {
                            if (!databasePlan.PartitionPlansByName.TryGetValue(partitionName, out var partitionPlan))
                            {
                                outputModel.Report(Diagnostics.Errors.NoGetPk, classModel.ClassSymbol);
                            }
                            else
                            {
                                var documentPlan = new DocumentPlan
                                {
                                    DocType = type,
                                    FullTypeName = classModel.ClassSymbol.ToDisplayString(),
                                    ClassModel = classModel,
                                    IsMutable = classModel.MutableAttribute is not null,
                                    IsTransient = classModel.TransientAttribute is not null,
                                    AutoExpires = classModel.TransientAttribute?.AutoExpires ?? false,
                                    DefaultTtl = classModel.TransientAttribute?.DefaultTtl,
                                    ClassName = name,
                                    ClassNameArgument = name.ToArgumentName(),
                                    PluralName = name.Pluralize(),
                                    ConstDocType = $"{databasePlan.Namespace}.{databasePlan.TypesClassName}.{partitionPlan.ClassName}.{name}",
                                    GetIdPlan = new GetPkIdPlan
                                    {
                                        FullMethodName = $"{getId.MethodSymbol.ContainingType.ToDisplayString()}.{getId.MethodSymbol.Name}",
                                        Arguments = getId
                                            .MethodSymbol
                                            .Parameters
                                            .Select(x => new GetPkIdPlan.Argument
                                            {
                                                ArgumentName = x.Name.ToArgumentName(),
                                                FullTypeName = x.Type.ToDisplayString()
                                            })
                                            .ToList()
                                    }
                                };

                                partitionPlan.Documents.Add(documentPlan);

                                outputModel.ValidateNames(
                                    classModel.ClassSymbol,
                                    documentPlan.ClassName,
                                    documentPlan.PluralName);
                                outputModel.ValidateIdentifiers(
                                    classModel.ClassSymbol,
                                    documentPlan.ClassNameArgument);

                                PropertyPlanBuilder.AddProperties(outputModel, classModel, documentPlan);

                                documentPlan.GetIdPlan = GetPkIdPlanBuilder.Build(outputModel, getId.MethodSymbol, documentPlan.PropertiesByName, documentPlan.PropertiesByArgumentName);
                                if (partitionPlan.GetPkPlan is null)
                                {
                                    partitionPlan.GetPkPlan = GetPkIdPlanBuilder.Build(outputModel, partitionPlan.GetPkModel.MethodSymbol, documentPlan.PropertiesByName, documentPlan.PropertiesByArgumentName);
                                }
                                else
                                {
                                    foreach (var arg in partitionPlan.GetPkPlan.Arguments)
                                    {
                                        if (arg.PropertyName is not null && !documentPlan.PropertiesByName.ContainsKey(arg.PropertyName))
                                        {
                                            outputModel.Report(Diagnostics.Errors.PropertyResolvePkIdConsistency, partitionPlan.GetPkModel.MethodSymbol, arg.ArgumentName, arg.PropertyName);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Models.Attributes;
using Cosmogenesis.Generator.Plans;
using Microsoft.CodeAnalysis;

namespace Cosmogenesis.Generator.PlanBuilders;
static class DatabasePlanBuilder
{
    public static void Build(OutputModel outputModel, OutputPlan outputPlan)
    {
        outputModel.CancellationToken.ThrowIfCancellationRequested();
        if (!outputModel.CanGenerate) { return; }

        var databasePlansByClass = new Dictionary<ClassModel, List<DatabasePlan>>();

        foreach (var dbAttribute in outputModel.DbAttributes)
        {
            Initialize(outputModel, null, outputPlan, dbAttribute);
        }
        foreach (var classModel in outputModel.Classes)
        {
            outputPlan.DatabasePlansByClass[classModel] = [];
            foreach (var dbAttribute in classModel.DbAttributes)
            {
                Initialize(outputModel, classModel, outputPlan, dbAttribute);
            }
        }
        var defaultDatabase = outputPlan.DatabasePlansByName.Count == 1 ? outputPlan.DatabasePlansByName.Values.Single() : null;
        if (defaultDatabase is not null)
        {
            foreach (var byClass in outputPlan.DatabasePlansByClass.Values.Where(x => x.Count == 0))
            {
                byClass.Add(defaultDatabase);
            }
        }
        foreach (var classModel in outputPlan.DatabasePlansByName.Values.GroupBy(x => x.Namespace).Where(x => x.Count() > 1).Select(x => x.First().ClassModel))
        {
            outputModel.Report(Diagnostics.Errors.DatabaseNamespaces, classModel?.ClassSymbol as ISymbol ?? outputModel.Compilation.Assembly);
        }

        PartitionPlanBuilder.AddPartitions(outputModel, outputPlan);
        DocumentPlanBuilder.AddDocuments(outputModel, outputPlan);
        PartitionPlanBuilder.RemoveEmptyPartitions(outputModel, outputPlan);
        UnionPlanBuilder.AddUnions(outputModel, outputPlan);

        foreach (var databasePlan in outputPlan.DatabasePlansByName.Values)
        {
            foreach (var documentPlan in databasePlan.PartitionPlansByName.Values.SelectMany(x => x.Documents).GroupBy(x => x.DocType).Where(x => x.Count() > 1).Select(x => x.First()))
            {
                outputModel.Report(Diagnostics.Errors.DuplicateDocType, documentPlan.ClassModel.ClassSymbol, documentPlan.DocType);
            }
            databasePlan.AllowTtl = databasePlan.PartitionPlansByName.Values.SelectMany(x => x.Documents).All(x => x.IsTransient);
            databasePlan.MustHaveTtl = databasePlan.PartitionPlansByName.Values.SelectMany(x => x.Documents).Any(x => x.AutoExpires && x.DefaultTtl != -1);
        }
    }
    static void Initialize(OutputModel outputModel, ClassModel? classModel, OutputPlan outputPlan, DbAttributeModel dbAttribute)
    {
        var name = dbAttribute.Name ?? "";
        var symbol = classModel?.ClassSymbol as ISymbol ?? outputModel.Compilation.Assembly;
        if (outputPlan.DatabasePlansByName.TryGetValue(name, out var plan))
        {
            if (dbAttribute.Namespace.NullIfEmpty() is not null &&
                dbAttribute.Namespace != plan.Namespace)
            {
                if (plan.IsDefaultNamespace)
                {
                    plan.IsDefaultNamespace = false;
                    plan.Namespace = dbAttribute.Namespace!;
                    plan.ClassModel = classModel;
                }
                else
                {
                    outputModel.Report(Diagnostics.Errors.DbMultipleNamespace, symbol);
                }
            }
        }
        else
        {
            plan = new DatabasePlan
            {
                Namespace = dbAttribute.Namespace.NullIfEmpty()!,
                Name = name,
                ClassModel = classModel,
                DbClassName = name.WithSuffix(Suffixes.Database),
                PartitionsClassName = name.WithSuffix(Suffixes.Partitions),
                QueryBuilderClassName = name.WithSuffix(Suffixes.QueryBuilder),
                QueryBuilderUnionsClassName = name.WithSuffix(Suffixes.QueryBuilderUnions),
                QueryClassName = name.WithSuffix(Suffixes.Query),
                QueryUnionsClassName = name.WithSuffix(Suffixes.QueryUnions),
                ReadClassName = name.WithSuffix(Suffixes.Read),
                SerializerClassName = name.WithSuffix(Suffixes.Serializer),
                ConverterClassName = name.WithSuffix(Suffixes.Converter),
                TypesClassName = name.WithSuffix(Suffixes.Types),
                BatchHandlersClassName = name.WithSuffix(Suffixes.BatchHandlers),
                ChangeFeedProcessorClassName = name.WithSuffix(Suffixes.ChangeFeedProcessor)
            };
            plan.BatchHandlersArgumentName = plan.BatchHandlersClassName.ToArgumentName();
            plan.IsDefaultNamespace = plan.Namespace is null;
            plan.Namespace ??= classModel?.ClassSymbol.ContainingNamespace?.ToDisplayString() ?? outputModel.Compilation.Assembly.Name.NullIfEmpty() ?? "Cosmogenesis.Generated";
            plan.DbClassNameArgument = plan.DbClassName.ToArgumentName();
            plan.QueryBuilderClassNameArgument = plan.QueryBuilderClassName.ToArgumentName();
            outputModel.ValidateNames(
                symbol,
                plan.Name,
                plan.DbClassName,
                plan.PartitionsClassName,
                plan.QueryBuilderClassName,
                plan.QueryClassName,
                plan.ReadClassName,
                plan.SerializerClassName,
                plan.ConverterClassName,
                plan.TypesClassName,
                plan.BatchHandlersClassName,
                plan.ChangeFeedProcessorClassName,
                plan.QueryBuilderUnionsClassName,
                plan.QueryUnionsClassName);
            outputModel.ValidateIdentifiers(
                symbol,
                plan.BatchHandlersArgumentName,
                plan.DbClassNameArgument,
                plan.QueryBuilderClassNameArgument);
            plan.Namespace.ValidateNamespace(outputModel, symbol);
            outputPlan.DatabasePlansByName[name] = plan;
        }
        if (classModel is not null)
        {
            outputPlan.DatabasePlansByClass[classModel].Add(plan);
        }
    }
}

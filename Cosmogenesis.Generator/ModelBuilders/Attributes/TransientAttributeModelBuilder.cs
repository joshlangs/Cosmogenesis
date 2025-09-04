using Cosmogenesis.Generator.Models;
using Cosmogenesis.Generator.Models.Attributes;
using Microsoft.CodeAnalysis;

namespace Cosmogenesis.Generator.ModelBuilders.Attributes;
static class TransientAttributeModelBuilder
{
    public static void Build(OutputModel outputModel, ClassModel classModel, AttributeData attributeData)
    {
        var model = new TransientAttributeModel();
        if (!attributeData.ConstructorArguments.IsDefaultOrEmpty)
        {
            foreach (var arg in attributeData.ConstructorArguments)
            {
                if (arg.Value is bool autoExpires)
                {
                    model.AutoExpires = autoExpires;
                }
                else if (arg.Value is int defaultTtl)
                {
                    model.DefaultTtl = defaultTtl;
                }
            }
            if (model.DefaultTtl.HasValue)
            {
                model.AutoExpires = true;
            }
        }
        classModel.TransientAttribute = model;
    }
}

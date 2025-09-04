using Cosmogenesis.Generator.Models;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Cosmogenesis.Generator;
static class ValidationExtensions
{
    public static void ValidateName(this string? name, OutputModel outputModel, ISymbol symbol)
    {
        if (ValidateName(name) is DiagnosticDescriptor diag)
        {
            outputModel.Report(diag, symbol, name);
        }
    }
    public static void ValidateNamespace(this string? @namespace, OutputModel outputModel, ISymbol symbol) => ValidateNames(outputModel, symbol, @namespace?.Split('.') ?? [null!]);
    static DiagnosticDescriptor? ValidateName(string? name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return Diagnostics.Errors.InvalidIdentifier;
        }
        if (!SyntaxFacts.IsValidIdentifier(name))
        {
            return Diagnostics.Errors.InvalidIdentifier;
        }
        if (!char.IsUpper(name[0]))
        {
            return Diagnostics.Errors.Uppercase;
        }
        return null;
    }
    static DiagnosticDescriptor? ValidateIdentifier(string? name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return Diagnostics.Errors.InvalidIdentifier;
        }
        if (!SyntaxFacts.IsValidIdentifier(name))
        {
            return Diagnostics.Errors.InvalidIdentifier;
        }
        return null;
    }
    public static void ValidateNames(this OutputModel outputModel, ISymbol symbol, params string[] names)
    {
        foreach (var name in names)
        {
            ValidateName(name, outputModel, symbol);
        }
    }
    public static void ValidateIdentifiers(this OutputModel outputModel, ISymbol symbol, params string[] names)
    {
        foreach (var name in names)
        {
            if (ValidateIdentifier(name) is DiagnosticDescriptor diag)
            {
                outputModel.Report(diag, symbol, name);
            }
        }
    }
    public static void ValidateDocType(this string? docType, OutputModel outputModel, ISymbol symbol)
    {
        if (string.IsNullOrWhiteSpace(docType) ||
            docType != docType!.Trim() ||
            docType.Contains('"') ||
            docType.Contains('\''))
        {
            outputModel.Report(Diagnostics.Errors.InvalidDocType, symbol, docType);
        }
    }
}

namespace Cosmogenesis.Generator;
static class StringExtensions
{
    public static string? NullIfEmpty(this string? s) => string.IsNullOrEmpty(s) ? null : s;

    public static string WithSuffix(this string str, string suffix) =>
        str is null ? "" :
        str.EndsWith(suffix) ? str :
        str + suffix;

    public static string WithoutSuffix(this string str, string suffix) =>
        str is null ? "" :
        str.EndsWith(suffix) ? str.Substring(0, str.Length - suffix.Length) :
        str;

    public static string ToArgumentName(this string name) =>
        string.IsNullOrEmpty(name) ? name :
        char.IsUpper(name[0]) ? char.ToLower(name[0]) + name.Substring(1) :
        '_' + name;

    static readonly string[] PluralEndings = ["s", "sh", "ch", "x", "z"];
    public static string Pluralize(this string singular) =>
        PluralEndings.Any(singular.EndsWith) ? $"{singular}es" :
        singular.EndsWith("y") && !singular.EndsWith("ay") && !singular.EndsWith("ey") && !singular.EndsWith("iy") && !singular.EndsWith("oy") && !singular.EndsWith("uy") ? singular.Substring(0, singular.Length - 1) + "ies" :
        $"{singular}s"; // This is 100% correct for all English words in existance forever without any exception, definitely for sure.

    public static string ToPascalCase(this string name) =>
        string.IsNullOrEmpty(name) ? name :
        char.IsLower(name[0]) ? char.ToUpper(name[0]) + name.Substring(1) :
        name;

    public static string JoinNonEmpty(this IEnumerable<string> strings, string join = ", ") => string.Join(join, strings.Where(x => !string.IsNullOrEmpty(x)));
}

namespace Test.Cosmogenesis.Core;

public static class EnumHelper<TEnum> where TEnum : Enum
{
    static readonly TEnum[] values = [.. Enum.GetValues(typeof(TEnum)).Cast<TEnum>()];
    public static IEnumerable<TEnum> Values => [.. values];
}

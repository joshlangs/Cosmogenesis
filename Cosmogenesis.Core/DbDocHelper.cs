namespace Cosmogenesis.Core;

public static class DbDocHelper
{
    public const int MaxIdBytes = 1024;
    public const char InvalidCharReplacement = '_';
    static readonly char[] InvalidIdChars = ['/', '\\', '?', '#'];

    public static string GetValidId(string id)
    {
        ArgumentNullException.ThrowIfNull(id);

        if (id.Length > MaxIdBytes) // only actually works if ascii
        {
            throw new ArgumentOutOfRangeException(nameof(id), $"id exceeds max length: {id}");
        }

        var index = id.IndexOfAny(InvalidIdChars, 0);
        if (index >= 0)
        {
            for (var x = 0; x < InvalidIdChars.Length; ++x)
            {
                id = id.Replace(InvalidIdChars[x], InvalidCharReplacement);
            }
        }
        return id;
    }
}

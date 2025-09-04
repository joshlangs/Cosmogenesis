namespace Cosmogenesis.TestDb1.Singers;

[DocType("SeaShantySinger")]
[Transient(true)]
public sealed class SeaShantySingerDoc : SingerDocBase
{
    public static string GetId(string firstName, string lastName) => $"SeaShanty: {firstName} {lastName}";
}

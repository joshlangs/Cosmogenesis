# Cosmogenesis

Cosmogenesis is a C# source generator for CosmosDB.

You define some documents, sprinkle in a couple [attribute]s, and Cosmogenesis spits out all the code to use it in CosmosDB.

## The Big Picture

- A *Database*...
  - Contains multiple *Partitions*
  - Is logically the same as a CosmosDB Container
  - Is defined using the [Db] attribute

- A *Partition*...
  - Contains multiple *Documents*
  - All share the same CosmosDB Partition Key
  - Is defined using the [Partition] attribute
  - Has a key calculated using a static method you define named `GetPk`

- A *Document*...
  - Is logically the same as a CosmosDB Document
  - Is a class derived from `DbDoc`
  - Has an CosmosDB `id` property calculated using a static method you define named `GetId`

By creating some documents and attributes, Cosmogenesis will create code handling:
- Database initialization
- Queries
  - Cross partition
  - Within a single partition
  - Filtering by document types (or not)
  - Supporting LINQ (additional filtering, projections, etc)
  - Transparent handling of continuation tokens
- Read, Create, Delete, Replace, ReadOrCreate, CreateOrReplace
- Batch operations
- Change feed handling
- Serialization

## Installation

Install both `Cosmogenesis.Core` and `Cosmogenesis` packages from Nuget.

## Usage

### CosmosDB Setup

Your CosmosDB container must have the following properties:
- Partition key = `/pk`
- Check the checkbox saying `My partition key is larger than 100 bytes`
- Use the default API (SQL), not Mongo, Cassandra, Gremlin, etc

### Project Setup

Create a `C#` `class library`

Install the `Cosmogenesis` and `Cosmogenesis.Core` Nuget packages.

Create some documents and sprinkle in some attributes

## Attributes

`[Db("DatabaseName", Namespace = "Optional.Namespace")]` to define a database.  If only 1 database exists (which is most convenient), then all documents by default go into it.  Otherwise, `[Db]` must appear on every document class.

`[Partition("PartitionName")]` to define a partition.  Attaching this to an abstract base class is often convenient, so all documents can derive from the base class and do not need to specify `[Partition]`.

`[Mutable]` marks a document which has properties that can change.  Without it, `Replace` functions will not be generated.

`[Transient]` marks a document which can be deleted.  Without it, `Delete` functions will not be generated.  Optionally sets a document's ttl to auto-expire.

`[UseDefault]` can be used on properties that don't always need initialization.  Methods like `Create` generate parameters with default values for you.

`[PartitionDefinition]` can be used on a static class containing static methods, each of which will define a new partition.  Instead of the implicit `GetPk` method, the static methods in this class are used to generate the partition keys.

`[DocType("SomeType")]` can be used to control the `Type` property which exists on all `DbDoc` documents.  By default, a class named `OrderDoc` would have a Type value of `Order`, but you can override this with [DocType].


## Examples

Define some documents (these are from `Cosmogenesis.TestDb4`):
```
[Partition("Accounts")]
public abstract class AccountDocBase : DbDoc
{
    public static string GetPk(Guid accountId) => $"Account={accountId:N}";

    public Guid AccountId { get; init; }
}

[Mutable]
public sealed class AccountInfoDoc : AccountDocBase
{
    public static string GetId() => "Info";

    public string Name { get; set; } = default!;
    public bool IsEvil { get; set; }
    public int MinionCount { get; set; }
}

[Transient]
[Mutable]
public sealed class PhoneNumberDoc : AccountDocBase
{
    public static string GetId(string phoneNumberType) => $"PhoneNumberType={phoneNumberType}";

    public string PhoneNumberType { get; init; } = default!;
    public string PhoneNumber { get; set; } = default!;
    public bool IsActive { get; set; }
}

[Partition("Orders")]
public sealed class OrderDoc : DbDoc
{
    public static string GetPk(Guid accountId) => $"Orders={accountId:N}";
    public static string GetId(string orderNumber) => $"Order={orderNumber}";

    public Guid AccountId { get; init; }
    public string OrderNumber { get; init; } = default!;

    public class Item
    {
        public string ItemCode { get; init; } = default!;
        public decimal UnitCost { get; init; }
        public long Quantity { get; init; }
    }

    public Item[] Items { get; init; } = default!;
    public List<string> Notes { get; init; } = default!;
    public decimal TotalPrice { get; init; }
}
```

Now, in some other app, you can add a reference to the project you just created.

Here is some sample code (from `Cosmogenesis.TestDb4.App`) so you get a flavor of how it gets consumed:

```
var cosmosClient = new CosmosClient("a connection string");
var container = cosmosClient.GetDatabase("a database name").GetContainer("a container name");

var db = new EvilCorpDb(container); // using Evil.Corp.Database;
await db.ValidateContainerAsync();

var acctId = Guid.NewGuid();

// Create a single AccountInfo document
var accountInfo = await db
    .Partition
    .Accounts(accountId: acctId)
    .Create
    .AccountInfoAsync(name: "Bob", isEvil: true, minionCount: 4)
    .ThrowOnConflict();  // using Cosmogenesis.Core;


// Update minion count and add two PhoneNumber documents, atomically in a batch
++accountInfo.MinionCount;
await db
    .Partition
    .Accounts(accountId: acctId) // We could have saved this value from before
    .CreateBatch()
    .CreatePhoneNumber(phoneNumberType: "Mobile", phoneNumber: "555-Evil", isActive: true)
    .CreatePhoneNumber(phoneNumberType: "Land", phoneNumber: "556-Evil", isActive: true)
    .Replace(accountInfo)
    .ExecuteOrThrowAsync(); // Explode if the batch fails
// The accountInfo variable is now "stale" and must be reloaded if more operations on it are needed
// To avoid this, we could have used ExecuteWithResultsAsync to get the new version of accountInfo

            
// Load any "Mobile" phone number for this account
var mobile = await db
    .Partition
    .Accounts(accountId: acctId)
    .Read
    .PhoneNumberAsync(phoneNumberType: "Mobile");

            
// And delete it if we found one
if (mobile is not null)
{
    await db
        .Partition
        .Accounts(accountId: acctId)
        .DeleteAsync(mobile)
        .ThrowOnConflict();
}


// Create an order for the account
await db
    .Partition
    .Orders(accountId: acctId)
    .Create
    .OrderAsync(
        accountId: acctId,
        orderNumber: DateTime.Now.ToString(),
        items: new[]
        {
            new OrderDoc.Item { ItemCode = "abc", Quantity = 1, UnitCost = 1.99m },
            new OrderDoc.Item { ItemCode = "def", Quantity = 100, UnitCost = 999 }
        },
        notes: new List<string>() { "This person is pretty evil", "Be careful!" },
        totalPrice: 99901.99m)
    .ThrowOnConflict();


// Load all "Land" phone numbers that are active for all accounts
var activeLandPhoneNumbers = await db
    .CrossPartitionQuery
    .PhoneNumbers(x => x.Where(p => p.IsActive && p.PhoneNumberType == "Land"))
    .ToListAsync();  // Install nuget package System.Linq.Async for this
```

Here's what the `OrderDoc` looks like inside CosmosDB, which the code above created:

```
{
    "AccountId": "7a8ca455-7346-4d85-ab01-28890e733b92",
    "OrderNumber": "3/19/2021 1:44:13 PM",
    "Items": [
        {
            "ItemCode": "abc",
            "UnitCost": "1.99",
            "Quantity": "1"
        },
        {
            "ItemCode": "def",
            "UnitCost": 999,
            "Quantity": "100"
        }
    ],
    "Notes": [
        "This person is pretty evil",
        "Be careful!"
    ],
    "TotalPrice": "99901.99",
    "pk": "Orders=7a8ca45573464d85ab0128890e733b92",
    "id": "Order=3_19_2021 1:44:13 PM",
    "_etag": "\"9b00a55d-0000-0a00-0000-60550d1c0000\"",
    "Type": "OrderDoc",
    "CreationDate": "2021-03-19T20:44:13.1272959Z",
    "_rid": "WWZSAPAwiFIEAAAAAAAAAA==",
    "_self": "dbs/WWZSAA==/colls/WWZSAPAwiFI=/docs/WWZSAPAwiFIEAAAAAAAAAA==/",
    "_attachments": "attachments/",
    "_ts": 1616186652
}
```

## Important Details

### Named Parameters

**USE NAMED PARAMETERS**!!!

The source generators do not guarantee the same parameter ordering.  Use named parameters to avoid nasty gotchas.

Example:  `Something(bool isOk, bool willDieImmediately);` without named parameters would be called like `Something(true, false)`.  Without warning, the source generator might change it to `Something(bool willDieImmediately, bool isOk);`, in which case your parameters are backwards.  Using `Something(isOk: true, willDieImmediately: false)` will always work without problems.

### Renaming

Don't rename document classes unless you really know what you're doing.  Serializing depends on the `Type` property.  The `Type` property is (by default) generated by the class name.

Don't rename document properties either.  Remember that the fields you use are saved in a database in JSON documents, and if you start renaming properties in your class, the serializer won't pick up those properties when deserializing existing data.

Don't change the `GetId` or `GetPk` methods after they've been defined.

## Serialization

Cosmogenesis uses some JSON converters by default to support common scenarios and handle some edge cases.

These conversions should be kept in mind while constructing queries.

### Byte Arrays

`byte[]` will be serialized into a hexadecimal string.  Always lowercase, always an even-number string length (2 hex digits per byte).

### Decimal, BigFraction, BigInteger and Int64 (long, ulong)

`long` `ulong` `decimal` `BigInteger` and `BigFraction` will be serialized into a string.

This avoids the large integer JSON data-loss problem and CosmosDB numeric type limitations.

But it also means these values are no longer natively sortable! "10" is less than "6", now that they're strings.  

Note:  float, double, and 32-bit integers (int, uint) are not converted - they are stored as JSON numbers and can be natively sorted by CosmosDB.

### DateTime

`DateTime` types are always stored in UTC time in ISO 8601 format with 7 digits of fractional seconds.

This avoids the CosmodDB date sorting problem due to minimized fractional digits.  See: https://github.com/Azure/azure-cosmos-dotnet-v3/issues/1468

When deserializing, `DateTime` properties will always be deserialized into UTC, even if they were originally set using a local time.

### DateOnly

`DateOnly` will be serialized to `yyyy-MM-dd` format.

### Enum

Cosmogenesis uses the built-in [JsonStringEnumConverter] converter with default values.

### IPAddress

IPAddress instances will be converted to and from string.

### Collections

Collections like List<T>, etc, are supported (by `MagicConverter`) - https://github.com/dotnet/corefx/issues/39477

### Custom

[JsonConverter] can be used as well.
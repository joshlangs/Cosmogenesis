using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cosmogenesis.Core;
using Cosmogenesis.TestDb4.Sales;
using Evil.Corp.Database;
using Microsoft.Azure.Cosmos;

namespace Cosmogenesis.TestDb4.App;

class Program
{
    static async Task Main()
    {
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
            .Accounts(accountId: acctId) // We could have cached this value
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
                orderNumber: DateTime.Now.ToString(),
                items:
                [
                    new OrderDoc.Item { ItemCode = "abc", Quantity = 1, UnitCost = 1.99m },
                    new OrderDoc.Item { ItemCode = "def", Quantity = 100, UnitCost = 999 }
                ],
                notes: ["This person is pretty evil", "Be careful!"],
                totalPrice: 99901.99m)
            .ThrowOnConflict();


        // Load all "Land" phone numbers that are active for all accounts
        var activeLandPhoneNumbers = await db
            .CrossPartitionQuery
            .PhoneNumbers(x => x.Where(p => p.IsActive && p.PhoneNumberType == "Land"))
            .ToListAsync();  // Install nuget package System.Linq.Async for this
    }
}

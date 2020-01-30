// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Samples.Bulk
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using Newtonsoft.Json;
    using System.Linq;    
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    // ----------------------------------------------------------------------------------------------------------
    // Prerequisites -
    //
    // 1. An Azure Cosmos account - 
    //    https://azure.microsoft.com/en-us/itemation/articles/itemdb-create-account/
    //
    // 2. Microsoft.Azure.Cosmos NuGet package - 
    //    http://www.nuget.org/packages/Microsoft.Azure.Cosmos/ 
    // ----------------------------------------------------------------------------------------------------------
    public class Program
    {
        private const string EndpointUrl = "https://<your-account>.documents.azure.com:443/";
        private const string AuthorizationKey = "<your-account-key>";
        private const string DatabaseName = "bulk-tutorial";
        private const string ContainerName = "items";
        private const int ItemsToInsert = 30000;
        private const int ItemsToDelete = 0;
        private const string PartitionKey = "/pk";
        private const int Throughput = 5000;
        private const int concurrentWorkers = 5;
        private const bool TryImport = true;
        private const bool TryDelete = true;
        private const int RunTimes = 3;
        private static readonly JsonSerializer Serializer = new JsonSerializer();

        static async Task Main(string[] args)
        {
            // <CreateClient>
            CosmosClient cosmosClient = new CosmosClient(EndpointUrl, AuthorizationKey, new CosmosClientOptions() { AllowBulkExecution = true });
            // </CreateClient>

            // Create with a throughput of specified RU/s
            // Indexing Policy to exclude all attributes to maximize RU/s usage
            Console.WriteLine($"This tutorial will create a {Throughput} RU/s container, press any key to continue.");
            Console.ReadKey();

            // <Initialize>
            Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync(Program.DatabaseName);
            await database.DefineContainer(Program.ContainerName, PartitionKey)
                    .WithIndexingPolicy()
                        .WithIndexingMode(IndexingMode.Consistent)
                        .WithIncludedPaths()
                            .Attach()
                        .WithExcludedPaths()
                            .Path("/*")
                            .Attach()
                    .Attach()
                .CreateAsync(Throughput);
            Container container = database.GetContainer(ContainerName);
            // </Initialize>

            Stopwatch stopwatch = new Stopwatch();            
            
            int iCounterSuccess = 0, iCounterFailed = 0;
            for (int i = 1; i <= RunTimes; i++)
            {
                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}, Round {i} starting ...");
                try
                {
                    if (TryImport)
                    {     
                        // Prepare items for insertion
                        Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}, Preparing {ItemsToInsert} items to insert into collection [{ContainerName}] ...");
                        // <Operations>
                        List<Item> ItemLists = Program.GetItemsToInsert(ItemsToInsert);
                        // </Operations>

                        Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}, Importing ... {ItemLists.Count} items");
                        stopwatch.Restart();
                        while (ItemLists.Count > 0)
                        {
                            Dictionary<PartitionKey, Stream> itemsToInsert = new Dictionary<PartitionKey, Stream>(ItemLists.Count);
                            foreach (Item item in ItemLists)
                            {
                                String tmpstr = JsonConvert.SerializeObject(item);
                                byte[] byteArray = Encoding.UTF8.GetBytes(tmpstr);
                                MemoryStream stream = new MemoryStream(byteArray);
                                itemsToInsert.Add(new PartitionKey(item.pk), stream);
                            }

                            // Create the list of Tasks
                            // <ConcurrentTasks>
                            List<Task> tasksInsert = new List<Task>(concurrentWorkers);
                            ResponseStatus ImportStatus = new ResponseStatus();
                            foreach (KeyValuePair<PartitionKey, Stream> item in itemsToInsert)
                            {
                                tasksInsert.Add(container.CreateItemStreamAsync(item.Value, item.Key)
                                    .ContinueWith((Task<ResponseMessage> task) =>
                                    {
                                        using (ResponseMessage response = task.Result)
                                        {
                                            if (!response.IsSuccessStatusCode)
                                            {
                                                ImportStatus.Add(response.StatusCode.ToString());
                                            }
                                        }
                                    }));
                            }

                            // Wait until all are done
                            await Task.WhenAll(tasksInsert);
                            iCounterSuccess = tasksInsert.Count(t => t.IsCompletedSuccessfully);
                            iCounterFailed = tasksInsert.Count(t => !t.IsCompletedSuccessfully);
                            // </ConcurrentTasks>

                            // <RetryIfNeeded>
                            if (ImportStatus.StatusCode.Count > 0)
                            {
                                foreach (Item item in await GetCollectionItemsAll(container))
                                {
                                    var itemToRemove = ItemLists.SingleOrDefault(r => r.id == item.id);
                                    ItemLists.Remove(itemToRemove);
                                }
                                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}, Some operations failed, received status code: {ImportStatus.ListStats()}.");
                                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}, Retrying ... {ItemLists.Count} items");
                            }
                            else
                            {
                                ItemLists.Clear();
                                itemsToInsert.Clear();
                            }
                            // </RetryIfNeeded>
                        }

                        stopwatch.Stop();
                        Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}, Finished in writing {ItemsToInsert} items in {stopwatch.Elapsed}. " +
                            $"({(ItemsToInsert * 1000.0 / stopwatch.ElapsedMilliseconds).ToString("f2")} items/sec)");
                    }

                    if (TryDelete)
                    {
                        // Prepare items for deletion
                        Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}, Fetching items in collection [{ContainerName}] to delete...");
                        // <Operations>
                        List<Item> itemsToDelete = new List<Item>();
                        itemsToDelete = await GetCollectionItemsAll(container);
                        int DeleteItemsCount = itemsToDelete.Count;
                        // </Operations>                        

                        Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}, Deleting ... {itemsToDelete.Count} items");
                        stopwatch.Restart();
                        while (itemsToDelete.Count > 0)
                        {
                            // Create the list of Tasks                            
                            // <ConcurrentTasks>
                            List<Task> tasksDelete = new List<Task>(concurrentWorkers);
                            ResponseStatus DeleteStatus = new ResponseStatus();
                            foreach (var item in itemsToDelete)
                            {                                
                                tasksDelete.Add(container.DeleteItemStreamAsync(item.id, new PartitionKey(item.pk))
                                    .ContinueWith((Task<ResponseMessage> task) =>
                                    {
                                        using (ResponseMessage response = task.Result)
                                        {
                                            if (!response.IsSuccessStatusCode)
                                            {
                                                DeleteStatus.Add(response.StatusCode.ToString());
                                            }
                                        }
                                    }));
                            }
                            // Wait until all are done
                            await Task.WhenAll(tasksDelete);
                            iCounterSuccess = tasksDelete.Count(t => t.IsCompletedSuccessfully);
                            iCounterFailed = tasksDelete.Count(t => !t.IsCompletedSuccessfully);
                            // </ConcurrentTasks>

                            // <RetryIfNeeded>
                            if (DeleteStatus.StatusCode.Count > 0)
                            {   
                                itemsToDelete = await GetCollectionItemsAll(container);
                                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}, Some operations failed, received status code: {DeleteStatus.ListStats()}.");
                                Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}, Retrying ... {itemsToDelete.Count} items");
                            }
                            else
                            {
                                itemsToDelete.Clear();
                            }
                            // </RetryIfNeeded>
                        }

                        stopwatch.Stop();
                        Console.WriteLine($"{ DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}, Finished in deleting {DeleteItemsCount} items in {stopwatch.Elapsed}. " +
                            $"({(DeleteItemsCount * 1000.0 / stopwatch.ElapsedMilliseconds).ToString("f2")} items/sec)");

                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
                finally
                {
                    Console.WriteLine();
                    //Console.ReadKey();
                }
            }

        }

        // <Bogus>
        private static List<Item> GetItemsToInsert(int ItemCount)
        {
            return new Bogus.Faker<Item>()
                .StrictMode(true)
                //Generate item
                .RuleFor(o => o.id, f => Guid.NewGuid().ToString()) //id
                .RuleFor(o => o.productid, f => f.Commerce.Ean13())
                .RuleFor(o => o.product, f => f.Commerce.Product())
                .RuleFor(o => o.productname, f => f.Commerce.ProductName())
                .RuleFor(o => o.department, f => f.Commerce.Department())
                .RuleFor(o => o.usermail, f => f.Internet.ExampleEmail())
                .RuleFor(o => o.username, f => f.Internet.UserName())
                .RuleFor(o => o.usercountry, f => f.Address.CountryCode())
                .RuleFor(o => o.userip, f => f.Internet.Ip())
                .RuleFor(o => o.useravatar, f => f.Internet.Avatar())
                .RuleFor(o => o.comments, f => f.Lorem.Text())         
                .RuleFor(o => o.create_date, f => f.Date.Recent().ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"))
                //----------
                .RuleFor(o => o.pk, (f, o) => o.productid) //partitionkey
                .Generate(ItemCount);
        }
        // </Bogus>

        // <Model>
        public class Item
        {
            public string id { get; set; }
            public string pk { get; set; }
            public string productid { get; set; }
            public string product { get; set; }
            public string productname { get; set; }
            public string department { get; set; }
            public string usermail { get; set; }
            public string username { get; set; }
            public string usercountry { get; set; }
            public string userip { get; set; }
            public string useravatar { get; set; }
            public string comments { get; set; }
            public string create_date { get; set; }
        }
        // </Model>

        public class ResponseStatus
        {
            public Dictionary<string, int> StatusCode = new Dictionary<string, int>();
            public void Add(string code)
            {
                if (!StatusCode.ContainsKey(code))
                {
                    StatusCode.Add(code, 1);
                }
                else
                {
                    StatusCode[code]++;
                }
            }
            public String ListStats()
            {
                /*
                    "TooManyRequests"   The throttled issue and SDK will retry after.
                    "RequestTimeout"    Client should be recoverable(meaning next requests on the same instance of the client should succeed). 
                                        In most cases, this either indicates connectivity issues between client and Cosmos DB service endpoints or resource overload on the client.
                */
                String StasString = "";
                foreach (KeyValuePair<string, int> kvp in StatusCode)
                {
                    StasString += $"{kvp.Key}({kvp.Value}), ";
                }
                return StasString.Substring(0, StasString.Length - 2);
            }
        }

        private static async Task<List<Item>> GetCollectionItemsAll(Container container)
        {
            List<Item> tobeDeleteFromStream = new List<Item>();
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            QueryDefinition query = new QueryDefinition(
                "select c.id, c.pk from c ");
            FeedIterator streamResultSet = container.GetItemQueryStreamIterator(
                query,
                requestOptions: new QueryRequestOptions()
                {
                    MaxItemCount = 5000,
                    MaxConcurrency = 6
                }
            );

            while (streamResultSet.HasMoreResults)
            {
                using (ResponseMessage responseMessage = await streamResultSet.ReadNextAsync())
                {
                    if (responseMessage.IsSuccessStatusCode)
                    {
                        dynamic streamResponse = FromStream<dynamic>(responseMessage.Content);
                        List<Item> salesOrders = streamResponse.Documents.ToObject<List<Item>>();
                        tobeDeleteFromStream.AddRange(salesOrders);
                    }
                    else
                    {
                        Console.WriteLine($"Query item from stream failed. Status code: {responseMessage.StatusCode} Message: {responseMessage.ErrorMessage}");
                    }
                }
            }
            Debug.WriteLine("{0}, Found {1} items in {2} ms"
                , DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")
                , tobeDeleteFromStream.Count
                , stopWatch.ElapsedMilliseconds
            );

            return tobeDeleteFromStream;
        }

        private static T FromStream<T>(Stream stream)
        {
            using (stream)
            {
                if (typeof(Stream).IsAssignableFrom(typeof(T)))
                {
                    return (T)(object)(stream);
                }

                using (StreamReader sr = new StreamReader(stream))
                {
                    using (JsonTextReader jsonTextReader = new JsonTextReader(sr))
                    {
                        return Program.Serializer.Deserialize<T>(jsonTextReader);
                    }
                }
            }
        }

    }
}

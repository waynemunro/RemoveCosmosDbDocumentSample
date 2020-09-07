using System;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

namespace RemoveCosmosDocumentSample
{
    class Program
    {
        // The Azure Cosmos DB endpoint for running this sample.
        private static readonly string EndpointUri = ConfigurationManager.AppSettings["EndPointUri"];

        // The primary key for the Azure Cosmos account.
        private static readonly string PrimaryKey = ConfigurationManager.AppSettings["PrimaryKey"];

        // The Cosmos client instance
        private static CosmosClient cosmosClient;


        public static async Task Main()
        {
            try
            {
                Console.WriteLine("Beginning operations...\n");
                Program p = new Program();
                await p.GetAndDeleteReportsAsync();

            }
            catch (CosmosException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}", de.StatusCode, de);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: {0}", e);
            }
            finally
            {
                cosmosClient.Dispose();
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }


        private async Task GetAndDeleteReportsAsync()
        {
            cosmosClient = new CosmosClient(EndpointUri, PrimaryKey);

            await cosmosClient.CreateDatabaseIfNotExistsAsync("reporting");

            Container containerByRid = cosmosClient.GetContainer("reporting", "reporting");

            var sqlQueryText = @"--SELECT * FROM c -- WHERE c.???"; // <-- your SQL here

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);

            FeedIterator<JObject> queryResultSetIterator = containerByRid.GetItemQueryIterator<JObject>(queryDefinition);

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<JObject> currentResultSet = await queryResultSetIterator.ReadNextAsync();

                var currentResultSetCount = currentResultSet.Count;

                foreach (JObject report in currentResultSet)
                {
                    string id = report["id"].ToString();
                    var retryCount = 0;

                retry: retryCount++;

                    try
                    {
                        Log($"Info : records remaining #{currentResultSetCount}");

                        ItemResponse<JObject> reportDeleteyResponse = await containerByRid.DeleteItemAsync<JObject>(id, PartitionKey.None);

                        currentResultSetCount--;

                        Log($"Info : Deleted Report [{id}] ");
                    }
                    catch (Exception ex)
                    {
                        Log($"Error : {ex.Message} ");

                        id = report["_rid"].ToString();

                        if (retryCount < 3)
                        {
                            Log($"Info : Retrying {retryCount}");
                            goto retry;
                        };

                        throw;
                    }
                }
            }
        }

        public static void Log(string logMessage)
        {
            var timestamp = $"{DateTime.Now.ToLongTimeString()} {DateTime.Now.ToLongDateString()}";
            Console.WriteLine($"{timestamp} : {logMessage}");
        }
    }
}

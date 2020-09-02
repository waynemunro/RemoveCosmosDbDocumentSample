using System;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Net.Http.Headers;
using System.IO;

namespace RemoveCosmosDocumentSample
{
    class Program
    {
        // The Azure Cosmos DB endpoint for running this sample.
        private static readonly string EndpointUri = ConfigurationManager.AppSettings["EndPointUri"];

        // The primary key for the Azure Cosmos account.
        private static readonly string PrimaryKey = ConfigurationManager.AppSettings["PrimaryKey"];

        // The Cosmos client instance
        private CosmosClient cosmosClient;

        // The database we will create
        private Database database;

        // The container we will create.
        private Container container;

        // The name of the database and container we will create
        private string databaseId = "reporting";
        private string containerId = "reporting";

        // <Main>
        public static async Task Main(string[] args)
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
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }
        // </Main>


        private async Task GetAndDeleteReportsAsync()
        {
            this.cosmosClient = new CosmosClient(EndpointUri, PrimaryKey);

            // Set a variable to the Documents path.
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);

            this.container = this.database.GetContainer(containerId);

            Container containerByRid = this.cosmosClient.GetContainer(databaseId, containerId);

            var sqlQueryText = @"SELECT * FROM c -- WHERE c.???"; // <-- your SQL here

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
                        Log($"Info : records remaining #{currentResultSetCount-- + 1}  " );
                        Log($"Info : try#{retryCount}  ");

                        ItemResponse<JObject> reportDeleteyResponse = await containerByRid.DeleteItemAsync<JObject>(id, PartitionKey.None);
                    }
                    catch (Exception ex)
                    {
                        Log($"Error : {ex.Message} ");

                        id = report["_rid"].ToString();

                        if (retryCount < 3)
                        {
                            Log($"Info : Retrying");
                            goto retry;
                        };

                        throw;
                    }

                    Log($"Info : Deleted Report [{id}] ");
                }
            }

        }

        public static void Log(string logMessage)
        {
            var timestamp = $"{DateTime.Now.ToLongTimeString()} {DateTime.Now.ToLongDateString()}";
            Console.WriteLine(timestamp);
            Console.WriteLine(logMessage);

            // TODO: write to file
            //StreamWriter w = File.AppendText("log.txt");
            //w.Write("\r\nLog Entry : ");
            //w.WriteLine(timestamp);
            //w.WriteLine("  :");
            //w.WriteLine($"  :{logMessage}");
            //w.WriteLine("-------------------------------");
        }

        public static void DumpLog(StreamReader r)
        {
            string line;
            while ((line = r.ReadLine()) != null)
            {
                Console.WriteLine(line);
            }
        }

    }
}

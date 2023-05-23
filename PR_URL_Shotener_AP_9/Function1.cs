using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Data.Tables;
using System.Linq;

namespace PR_URL_Shotener_AP_9
{
    public class Function1
    {
        //private TableClient tableClient;
        private readonly ILogger<Function1> logger;
        private string _tableName = "shortUrl";

        public Function1(ILogger<Function1> logger) {
            this.logger = logger;
        }

        //private async Task GetTableClient()
        //{
        //    if(tableClient == null)
        //    {
        //        logger.LogInformation("Creating Table Client");
        //        string connStr = "UseDevelopmentStorage=true";
        //        tableClient = new TableClient(connStr, _tableName);
        //        await tableClient.CreateIfNotExistsAsync();
        //    }
        //}

        [FunctionName("Set")]
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            [Table("shortUrl")]TableClient tableClient,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string href = req.Query["href"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            href ??= data?.href;
     
            if(string.IsNullOrEmpty(href))
            {
                return new OkObjectResult(new { message = "Please send href parameter via body or query like http://localhost:7032/api/set?href=https://logbook.itstep.org/#/presents" });
            }
      

            //await GetTableClient();
            UrlKey urlKey;
            var result = await tableClient.GetEntityIfExistsAsync<UrlKey>("1", "Key");

            if(!result.HasValue) 
            {
                urlKey = new UrlKey() { Id = 1024, PartitionKey = "1", RowKey = "Key"};
                await tableClient.UpsertEntityAsync(urlKey);
            }
            else
            {
                urlKey = result.Value;
            }

            int index = urlKey.Id;
            string code = "";
            string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            while(index > 0)
            {
                code += alphabet[index % alphabet.Length];
                index /= alphabet.Length;
            }
            code  = string.Join(string.Empty, code.Reverse());
            UrlData urlData = new ()
            {
                Id = code,
                RowKey = code,
                PartitionKey = code[0].ToString(),
                Url = href,
                Count = 1
            };

            urlKey.Id++;
            await tableClient.UpsertEntityAsync(urlData);
            await tableClient.UpsertEntityAsync(urlKey);
            return new OkObjectResult(new {href, shortUrl = urlData.RowKey});
        }

        [FunctionName("Go")]
        public async Task<IActionResult> Go(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "go/{shortUrl}")]HttpRequest req,
            string shortUrl,
            [Table("shortUrl")]TableClient tableClient,
            [Queue("counts")]IAsyncCollector<string> queue,
            ILogger logger
            )
        {
            if (string.IsNullOrEmpty(shortUrl))
            {
                return new BadRequestResult();
            }

            shortUrl = shortUrl.ToUpper();
            var result = await tableClient.GetEntityIfExistsAsync<UrlData>(shortUrl[0].ToString(), shortUrl);
            if (!result.HasValue) 
            {
                return new BadRequestObjectResult(new {message = "There`s no such short url"});
            }
            await queue.AddAsync(result.Value.RowKey);
            return new RedirectResult(result.Value.Url);
        }

        [FunctionName("ProccessQueue")]
        public async Task ProccessQueue(
            [QueueTrigger("counts")]string shortCode,
            [Table("shortUrl")]TableClient tableClient,
            ILogger logger)
        {
            var result = await tableClient.GetEntityIfExistsAsync<UrlData>(shortCode[0].ToString(), shortCode);
            result.Value.Count++;
            await tableClient.UpsertEntityAsync(result.Value);
            logger.LogInformation($"From Short Url: {result.Value.RowKey} redirected {result.Value.Count} times");
        }
    }
}

using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace ScrappyFunctionApp
{
    public class ScheduledFunctions
    {
        const string ConnStr = "DefaultEndpointsProtocol=https;AccountName=scrappystorage;AccountKey=A9HIHQ4KuqEi5qnaORPP8knhQ6LehVN8gOLUmAU+ugo1KJKyQVWSKSNsWhnjm+kZOy5NoEHZPF+y+AStTu3FWQ==;EndpointSuffix=core.windows.net";
        public ScheduledFunctions(ILoggerFactory loggerFactory, IConfiguration configuration)
        {
        }

        [FunctionName("ScrapeSites")]
        // "0 0 */2 * * *" every two hours
        public async Task ScrapeSites([TimerTrigger("0 0 */1 * * *", RunOnStartup = true)] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"ScrapeSites function executed at: {DateTime.Now}");
            ScraperClass scraper = new ScraperClass(ConnStr, log);
            await scraper.ScrapeAll();
        }

        [FunctionName("CompareSiteData")]
        // "0 0 */2 * * *" every two hours
        public async Task CompareSiteData([TimerTrigger("0 0 */1 * * *", RunOnStartup = true)] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"CompareSiteData function executed at: {DateTime.Now}");
            CompareClass comparer = new CompareClass(ConnStr, log);
            await comparer.CompareAll();
        }

    }
}

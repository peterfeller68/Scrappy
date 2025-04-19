using AzureDataImportLibrary;
using Microsoft.Extensions.Logging;
using ScrappyFunctionApp.Data;
using System;
using System.Threading.Tasks;

namespace ScrappyFunctionApp
{
    public class CompareClass
    {
        static string BLOBCONTAINER = "scrappy";
        string _connStr;
        ILogger _log;

        public CompareClass(string ConnStr, ILogger log)
        {
            _connStr = ConnStr;
            _log = log;
        }

        protected async Task<string> LoadFromBlob(string blobBaseName)
        {
            BlobFunctions blobFunctions = new BlobFunctions(_connStr);
            string blobOutputName = $"{blobBaseName}.output";

            string output = await blobFunctions.Load(BLOBCONTAINER, blobOutputName);
            return output;
        }

        protected async Task<bool> CompareSiteAndSaveResult(SitesClass site)
        {
            SitesHistoryList siteHistList = new SitesHistoryList(_connStr);
            await siteHistList.LoadTop2(site);
            if (siteHistList.Count != 2)
            {
                throw new Exception($"Don't have two entries to compare.");
            }

            SitesHistoryClass curr = siteHistList[0] as SitesHistoryClass;
            if (curr.Processed == true)
            {
                throw new Exception($"The current entry {curr.RowKey} has already been processed.");
            }
            SitesHistoryClass prev = siteHistList[1] as SitesHistoryClass;

            string CompareResult = "N/A";
            string CompareError = "";
            string currOutput = await LoadFromBlob(curr.BlobBaseName);
            string prevOutput = await LoadFromBlob(prev.BlobBaseName);

            if (currOutput == prevOutput)
            {
                CompareResult = $"The content matches.";
                if (_log != null) _log.LogInformation($"{site.Label}, {curr.RowKey}, {prev.RowKey} - {CompareResult}");

                curr.Processed = true;
                await curr.SaveToTable_Upsert();
                return false;
            }
            else
            {
                try
                {
                    OpenAIClass chatGpt = new OpenAIClass(_log);
                    CompareResult = await chatGpt.CompareTwoStrings(site, currOutput, prevOutput);
                }
                catch (Exception e)
                {
                    CompareResult = "Error";
                    CompareError = e.Message;
                    if (_log != null) _log.LogError($"{site.Label}, {curr.RowKey}, {prev.RowKey} EXCEPTION - {CompareError}");

                }
            }

            CompareSitesHistoryClass compareResult = new CompareSitesHistoryClass(_connStr, true);
            compareResult.Init(site, curr.BlobBaseName, prev.BlobBaseName, CompareResult, CompareError);
            await compareResult.SaveToTable_Upsert();

            curr.Processed = true;
            await curr.SaveToTable_Upsert();

            return (currOutput != prevOutput);
        }

        public async Task CompareAll()
        {
            // iterate through all urls in the luSites table and compare the last two files
            SitesList sites = new SitesList(_connStr, true);
            await sites.LoadAllAsync();

            foreach (SitesClass site in sites)
            {
                if (site.Active)
                {
                    if (_log != null) 
                        _log.LogInformation ($"{site.Label}, Compare");
                    else
                        Console.WriteLine($"Comparing {site.Label}");
                    try
                    {
                        bool ret = await CompareSiteAndSaveResult(site);
                    }
                    catch (Exception ex)
                    {
                        if (_log != null) 
                            _log.LogError($"{site.Label}, Comparing Exception - {ex.Message}");
                        else
                            Console.WriteLine($"Failed to process {site.Label} - {ex.Message}");
                    }
                }
            }
        }
    }
}

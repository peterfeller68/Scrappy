using AzureDataImportLibrary;
using HtmlAgilityPack;
using ScrappyFunctionApp.Data;
using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace ScrappyFunctionApp
{
    public class ScraperClass
    {

        static string BLOBCONTAINER = "scrappy";
        string _connStr;
        readonly HttpClient _httpClient;
        ILogger _log;

        public ScraperClass(string ConnStr, ILogger log) 
        {
            _connStr = ConnStr;
            _httpClient = new HttpClient();
            _log = log;
        }

        protected async Task<string> GetPageAsync(string url)
        {
            _httpClient.DefaultRequestHeaders.Add("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");

            HttpResponseMessage response = await _httpClient.GetAsync(url);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync();
        }

        protected string ParseHtml(string html, string nodeSel)
        {
            StringBuilder ret = new StringBuilder();
            HtmlDocument document = new HtmlDocument();
            document.LoadHtml(html);

            if (!string.IsNullOrEmpty(nodeSel))
            {
                string path = $"//{nodeSel}";
                var nodes = document.DocumentNode.SelectNodes(path);
                foreach (var node in nodes)
                {
                    ret.Append(node.InnerText);
                }
                return ret.ToString();
            }
            return html;
        }

        protected async Task<string> saveToBlob(string html, string parsedHtml)
        {
            BlobFunctions blobFunctions = new BlobFunctions(_connStr);
            string blobBaseName = Guid.NewGuid().ToString();
            await blobFunctions.Save(BLOBCONTAINER, $"{blobBaseName}.html", html);
            await blobFunctions.Save(BLOBCONTAINER, $"{blobBaseName}.output", parsedHtml);
            return blobBaseName;
        }

        protected async Task saveToTable(SitesClass site, string blobBaseName)
        {
            SitesHistoryClass siteEntry = new SitesHistoryClass(_connStr, true);
            siteEntry.Init(site, blobBaseName);
            await siteEntry.SaveToTable_Upsert();
        }

        protected async Task ScrapeSiteAndSaveResult (SitesClass site)
        {
            string html = await GetPageAsync(site.Url);
            string parsedString = ParseHtml(html, site.Nodes);
            string blobBaseName = await saveToBlob(html, parsedString);
            await saveToTable(site, blobBaseName);
        }

        public async Task ScrapeAll()
        {
            // iterate through all urls in the luSites table and save off the results in the SitesHistory table
            SitesList sites = new SitesList(_connStr, true);
            await sites.LoadAllAsync();

            foreach (SitesClass site in sites)
            {
                if (site.Active)
                {
                    if (_log != null)
                        _log.LogInformation($"{site.Label}, Scraping");
                    else
                        Console.WriteLine($"Scraping {site.Label}");
                    try
                    {
                        await ScrapeSiteAndSaveResult(site);
                    }
                    catch (Exception ex)
                    {
                        if (_log != null)
                            _log.LogError($"{site.Label}, Scraping Exception - {ex.Message}");
                        else
                            Console.WriteLine($"Failed to process {site.Label} - {ex.Message}");
                    }
                }
            }
        }
    }
}

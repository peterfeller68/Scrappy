using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using HtmlAgilityPack;
using static System.Net.WebRequestMethods;

//public class PythonNetExample
//{
//    public void ExecutePythonCode(string url)
//    {
//        // Initialize the Python engine
//        PythonEngine.Initialize();
//        using (Py.GIL())
//        {
//            // Import the Python file
//            dynamic calculatorModule = Py.Import("Scappy");

//            // Create an instance of the Calculator class
//            dynamic calculator = calculatorModule.Calculator();

//            // Call the AddInPython method
//            string ret = calculator.scrape_data_nys_apg_modifiers(url);
//            Console.WriteLine(ret);
//        }

//        // Shutdown the Python engine
//        PythonEngine.Shutdown();
//    }
//}

// Example usage
class Program
{
//    request_headers = {
//    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
//}

    private static readonly HttpClient client = new HttpClient();
    public static async Task<string> GetPageAsync(string url)
    {
        client.DefaultRequestHeaders.Add("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");

        HttpResponseMessage response = await client.GetAsync(url);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStringAsync();
    }

    public static void ParseHtml(string html)
    {
        HtmlDocument document = new HtmlDocument();
        document.LoadHtml(html);
        var nodes = document.DocumentNode.SelectNodes("//table");
        foreach (var node in nodes)
        {
            Console.WriteLine(node.InnerText);
        }
    }

    static async Task Main()
    {
        string html = await GetPageAsync("https://www.health.ny.gov/health_care/medicaid/rates/methodology/modifiers.htm");
        ParseHtml(html);

    }

    //# https://www.health.ny.gov/health_care/medicaid/rates/methodology/modifiers.htm

}
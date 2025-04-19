using ScrappyFunctionApp;

namespace ScrappyTestClient
{
    public partial class Form1 : Form
    {
        const string ConnStr = "DefaultEndpointsProtocol=https;AccountName=scrappystorage;AccountKey=A9HIHQ4KuqEi5qnaORPP8knhQ6LehVN8gOLUmAU+ugo1KJKyQVWSKSNsWhnjm+kZOy5NoEHZPF+y+AStTu3FWQ==;EndpointSuffix=core.windows.net";
        public Form1()
        {
            InitializeComponent();
        }

        private async void button2_Click(object sender, EventArgs e)
        {
            ScraperClass scraper = new ScraperClass(ConnStr, null);
            await scraper.ScrapeAll();
        }

        private async void button1_Click(object sender, EventArgs e)
        {
            CompareClass comparer = new CompareClass(ConnStr, null);
            await comparer.CompareAll();
        }
    }
}

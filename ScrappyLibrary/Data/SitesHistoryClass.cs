using System;
using Azure.Data.Tables;
using AzureDataImportLibrary;
using System.Threading.Tasks;
using static OpenQA.Selenium.PrintOptions;

#nullable enable
namespace ScrappyFunctionApp.Data
{
    public class SitesHistoryClass : CustomTableEntity
    {
        public static string getRowKey()
        {
            return (DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks).ToString("d19");
        }

        public SitesHistoryClass(string ConnStr, bool bCreateIfNotExists = false)
            : base(ConnStr, SitesHistoryTable.TABLENAME(), bCreateIfNotExists)
        {
        }

        public SitesHistoryClass(StorageTable table, TableEntity ent)
          : base(table, ent)
        {
        }

        protected override void InitExcludedColumns() => base.InitExcludedColumns();

        public void Init(SitesClass site, string blobBaseName)
        {
            Url = site.Url;
            Label = site.Label;
            BlobBaseName = blobBaseName;
            RowKey = getRowKey(); //DateTime.UtcNow.ToString("yyyyMMdd"); //getRowKey();
            PartitionKey = site.Label; 
        }

        #region attributes
        public string Url { get; set; }
        public string BlobBaseName { get; set; }
        public string Label { get; set; }
        public bool Processed { get; set; }
        #endregion
    }

    public class SitesHistoryList : CustomTableEntityList
    {
        public SitesHistoryList(string storageKey, bool bCreateIfNotExists = false)
            : base(storageKey, SitesHistoryTable.TABLENAME(), bCreateIfNotExists)
        {
        }

        protected override CustomTableEntity GetEntity(TableEntity ent)
        {
            return new SitesHistoryClass(_tbl, ent);
        }

        public async Task LoadTop2(SitesClass site)
        {
            string str = StorageTable.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, site.Label);
            await LoadFirstPagedQueryAsync(str, 2);
        }

        public async Task LoadForLastxHours(string jobID, int nHours)
        {
            //await this.LoadFromQueryAsync(
            //   StorageTable.CombineFilters(
            //       StorageTable.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, jobID)
            //       , "and"
            //       , StorageTable.GenerateTimestampFilter(DateTime.UtcNow.AddHours(nHours * -1), DateTime.UtcNow)));
        }

    }

    public class SitesHistoryTable : StorageTable
    {
        public SitesHistoryTable(string storageKey, bool bSandbox, bool bCreateIfNotExists = false)
            : base(storageKey, TABLENAME(), bCreateIfNotExists)
        {
        }
        public static string TABLENAME()
        {
            return ValidStorageTableName("SitesHistory");
        }
    }

}

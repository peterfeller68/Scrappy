using System;
using Azure.Data.Tables;
using AzureDataImportLibrary;
using System.Threading.Tasks;

#nullable enable
namespace ScrappyFunctionApp.Data
{
    public class SitesClass : CustomTableEntity
    {
        public static string getRowKey()
        {
            return (DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks).ToString("d19");
        }

        public SitesClass(string ConnStr, bool bCreateIfNotExists = false)
            : base(ConnStr, SitesTable.TABLENAME(), bCreateIfNotExists)
        {
        }

        public SitesClass(StorageTable table, TableEntity ent)
          : base(table, ent)
        {
        }

        protected override void InitExcludedColumns() => base.InitExcludedColumns();


        #region attributes
        public string Url { get; set; }
        public string Nodes { get; set; }
        public bool Active { get; set; }
        public string Label { get; set; }
        public string GptPrompt { get; set; }
        #endregion
    }

    public class SitesList : CustomTableEntityList
    {
        public SitesList(string storageKey, bool bCreateIfNotExists = false)
            : base(storageKey, SitesTable.TABLENAME(), bCreateIfNotExists)
        {
        }

        protected override CustomTableEntity GetEntity(TableEntity ent)
        {
            return new SitesClass(_tbl, ent);
        }

        public async Task LoadByJobIDAndJobResultIDAsync(string jobID, string JobResultId)
        {
            //await this.LoadFromQueryAsync(
            //    StorageTable.CombineFilters(
            //        StorageTable.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, jobID)
            //        , "and"
            //        , StorageTable.GenerateFilterCondition("JobResultID", QueryComparisons.Equal, JobResultId)));
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

    public class SitesTable : StorageTable
    {
        public SitesTable(string storageKey, bool bSandbox, bool bCreateIfNotExists = false)
            : base(storageKey, TABLENAME(), bCreateIfNotExists)
        {
        }
        public static string TABLENAME()
        {
            return ValidStorageTableName("luSites");
        }
    }

}

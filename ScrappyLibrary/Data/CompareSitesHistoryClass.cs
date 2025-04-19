using Azure.Data.Tables;
using AzureDataImportLibrary;
using System;
using System.Threading.Tasks;

#nullable enable
namespace ScrappyFunctionApp.Data
{
    public class CompareSitesHistoryClass : CustomTableEntity
    {
        public static string getRowKey()
        {
            return (DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks).ToString("d19");
        }

        public CompareSitesHistoryClass(string ConnStr, bool bCreateIfNotExists = false)
            : base(ConnStr, CompareSitesHistoryTable.TABLENAME(), bCreateIfNotExists)
        {
        }

        public CompareSitesHistoryClass(StorageTable table, TableEntity ent)
          : base(table, ent)
        {
        }

        protected override void InitExcludedColumns() => base.InitExcludedColumns();

        public void Init(SitesClass site, string currBlobName, string prevBlobName, string compareResult, string compareError)
        {
            RowKey = getRowKey(); //DateTime.UtcNow.ToString("yyyyMMdd"); //getRowKey();
            PartitionKey = site.Label;

            Url = site.Url;
            Label = site.Label;
            CompareResult = compareResult;
            CompareError = compareError;
            CurrBlobName = currBlobName;
            PrevBlobName = prevBlobName;
        }

        #region attributes
        public string Url { get; set; }
        public string CurrBlobName { get; set; }
        public string PrevBlobName { get; set; }
        public string Label { get; set; }
        public string CompareResult { get; set; }
        public string CompareError { get; set; }
        #endregion
    }

    public class CompareSitesHistoryList : CustomTableEntityList
    {
        public CompareSitesHistoryList(string storageKey, bool bCreateIfNotExists = false)
            : base(storageKey, CompareSitesHistoryTable.TABLENAME(), bCreateIfNotExists)
        {
        }

        protected override CustomTableEntity GetEntity(TableEntity ent)
        {
            return new CompareSitesHistoryClass(_tbl, ent);
        }

        public async Task LoadTop(SitesClass site)
        {
            string str = StorageTable.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, site.Label);
            await LoadFirstPagedQueryAsync(str, 1);
        }

    }

    public class CompareSitesHistoryTable : StorageTable
    {
        public CompareSitesHistoryTable(string storageKey, bool bSandbox, bool bCreateIfNotExists = false)
            : base(storageKey, TABLENAME(), bCreateIfNotExists)
        {
        }
        public static string TABLENAME()
        {
            return ValidStorageTableName("CompareSitesHistory");
        }
    }

}

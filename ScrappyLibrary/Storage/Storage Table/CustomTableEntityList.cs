using Azure.Data.Tables;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;

#nullable disable
namespace AzureDataImportLibrary
{
    public class CustomTableEntityList : List<CustomTableEntity>
    {
        protected StorageTable _tbl;

        protected virtual CustomTableEntity GetEntity(TableEntity end) => (CustomTableEntity)null;

        public StorageTable Table => this._tbl;

        public CustomTableEntityList()
        {
            Init();
        }

        public CustomTableEntityList(StorageTable tbl)
        {
            this._tbl = new StorageTable(tbl.ConnectionString, tbl._tableName);
            Init();
        }

        public CustomTableEntityList(string storageKey, string TableName, bool bCreateIfNotExists = false)
        {
            this._tbl = new StorageTable(storageKey, TableName, bCreateIfNotExists);
            Init();
        }

        private void Init()
        {
            ExportColumns = new List<string>();
            ExportFormats = new List<string>();
            ExportColumnLabel = new List<string>();
        }

        #region Export
        public List<String> ExportColumns { get; set; }
        public List<String> ExportFormats { get; set; }
        public List<String> ExportColumnLabel{ get; set; }

        public string ExportToHtmlWithLinkedResult(CustomTableEntityList linkedData, String origKey, String secondKey)
        {
            return _ExportToHtml(linkedData, origKey, secondKey);
        }

        public string ExportToHtml()
        {
            return _ExportToHtml(null, "", "");
        }

        protected void AddTableRowHtml(ref string strHtml, List<String> data, bool bold=false)
        {
            strHtml += "<tr>";
            foreach (string col in data)
            {
                if (!bold)
                {
                    strHtml += $"<td>{col}</td>";
                }
                else
                {
                    strHtml += $"<td><b>{col}</b></td>";
                }
            }
            strHtml += "</tr>";

        }

        protected virtual void AddColumnValue(ref List<string> columns, string Value, CustomTableEntity entity)
        {
            columns.Add(Value);
        }

        protected void AddLinkedResult(ref List<String> colVals, CustomTableEntity primaryEntity, CustomTableEntityList linkedData, String origKey, String secondKey)
        {
            // find the row in the linked data set
            foreach (PropertyInfo property in primaryEntity.GetType().GetProperties((BindingFlags)52))
            {
                if (property.Name == origKey)
                {
                    foreach (CustomTableEntity linkedEntity in (List<CustomTableEntity>)linkedData)
                    {
                        foreach (PropertyInfo linkedProp in linkedEntity.GetType().GetProperties((BindingFlags)52))
                        {
                            if (linkedProp.Name == secondKey)
                            {
                                string origKeyVal = property.GetValue(primaryEntity).ToString();
                                string linkedPropKeyVal = linkedProp.GetValue(linkedEntity).ToString();
                                if (origKeyVal == linkedPropKeyVal)
                                {
                                    int index = 0; 
                                    foreach (string colName in linkedData.ExportColumns)
                                    {
                                        string outputString = "";
                                        if (GetFormattedPropertyString(ref outputString, index, colName, linkedEntity, linkedData.ExportFormats) == false)
                                        {
                                            outputString = $"'{colName}' Not Found.";
                                        }
                                        AddColumnValue(ref colVals, outputString, primaryEntity);
                                        index++;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        protected bool GetFormattedPropertyString(ref string outputString, int index, string propName, CustomTableEntity customTableEntity, List<string> Formats)
        {
            foreach (PropertyInfo property in customTableEntity.GetType().GetProperties((BindingFlags)52))
            {
                if (property.Name == propName)
                {
                    object val = property.GetValue(customTableEntity);
                    if (val != null)
                    {
                        outputString = property.GetValue(customTableEntity).ToString();
                    }
                    else
                    {
                        outputString = "(null)";
                    }

                    // get the format
                    string formatString = "";
                    if (Formats != null)
                    {
                        if (index < Formats.Count)
                        {
                            formatString = Formats[index];
                        }

                        if (property.PropertyType.FullName.Contains("Decimal"))
                        {
                            Decimal prop = (Decimal)property.GetValue(customTableEntity);
                            outputString = prop.ToString(formatString);
                        }
                        else if (property.PropertyType == typeof(DateTime))
                        {
                            DateTime prop = (DateTime)property.GetValue(customTableEntity);
                            outputString = prop.ToString(formatString);
                        }
                    }

                    return true;
                }
            }
            return false;
        }

        protected string _ExportToHtml(CustomTableEntityList linkedData, String origKey, String secondKey)
        {
            //< table class="tg"><thead>
            //  <tr>
            //    <td class="tg-0lax"></td>
            //  </tr></thead>
            //</table>
            string strHtml = "<table border=\"1\">";
            try
            {
                List<string> headerList = new List<String>(); 
                headerList.AddRange(ExportColumns);
                if (ExportColumnLabel.Count == ExportColumns.Count)
                {
                    headerList = ExportColumnLabel;
                }
                if (linkedData != null)
                {
                    if (linkedData.ExportColumnLabel.Count == linkedData.ExportColumns.Count)
                    {
                        headerList.AddRange(linkedData.ExportColumnLabel);
                    }
                    else
                    {
                        headerList.AddRange(linkedData.ExportColumns);
                    }
                }
                AddTableRowHtml(ref strHtml, headerList, true);

                CustomTableEntityList customTableEntityList = this;
                foreach (CustomTableEntity customTableEntity in (List<CustomTableEntity>)customTableEntityList)
                {
                    if (customTableEntity != null)
                    {
                        if (customTableEntity.UseInHTMLExport())
                        {
                            List<String> colVals = new List<String>();

                            int index = 0;
                            foreach (string colName in ExportColumns)
                            {

                                string outputString = "";
                                if (GetFormattedPropertyString(ref outputString, index, colName, customTableEntity, ExportFormats) == false)
                                {
                                    outputString = $"'{colName}' Not Found.";
                                }
                                AddColumnValue(ref colVals, outputString, customTableEntity);
                                index++;
                            }

                            if (linkedData != null)
                            {
                                AddLinkedResult(ref colVals, customTableEntity, linkedData, origKey, secondKey);
                            }

                            AddTableRowHtml(ref strHtml, colVals);
                        }
                    }
                }
                strHtml += "</table>";
            }
            catch (Exception ex)
            {
                strHtml = $"Failed generating the HTML output [{ex.Message}]";
            }
            return strHtml;
        }
        #endregion

        public CustomTableEntityList DeepCopy(List<TableEntity> lst)
        {
            CustomTableEntityList newLst = new CustomTableEntityList();
            foreach (TableEntity item in lst)
            {
                newLst.Add(GetEntity(item));
            }
            return newLst;
        }


        public async Task LoadForToday()
        {
            DateTime today = DateTime.UtcNow.Date;
            DateTime fromDate = new DateTime(today.Year, today.Month, today.Day, 0, 0, 0, DateTimeKind.Utc);
            await this.LoadFromQueryAsync(StorageTable.GenerateTimestampFilter(fromDate, DateTime.UtcNow));
        }

        public async Task LoadByTimeStampAsync(DateTime from, DateTime to)
        {
            await this.LoadFromQueryAsync(StorageTable.GenerateTimestampFilter(from, to));
        }

        public async Task LoadByRowKeyAsync(string rowKey)
        {
            await this.LoadFromQueryAsync(StorageTable.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));
        }

        public async Task LoadByPartitionKeyAsync(string partitionKey)
        {
            await this.LoadFromQueryAsync(StorageTable.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
        }

        public void Append(CustomTableEntityList list)
        {
            foreach (CustomTableEntity customTableEntity in (List<CustomTableEntity>)list)
                this.Add(customTableEntity);
        }

        public async Task LoadFromQueryAsync(string finalFilter)
        {
            Clear();
            await AppendFromQueryAsync(finalFilter);
        }

        protected async Task AppendFromQueryAsync(string finalFilter)
        {
            List<TableEntity> lst = await _tbl.GetRowsByFilterAsync(finalFilter);
            foreach (TableEntity end in lst)
            {
                CustomTableEntity entity = GetEntity(end);
                // ISSUE: explicit non-virtual call
                Add(entity);
            }
        }

        public async Task LoadAllAsync() => await this.LoadFromQueryAsync("");

        public async Task LoadByPartitionKeyAndDateRangeAsync(
            string partitionKey,
            string dateFieldName,
            DateTime from,
            DateTime to)
        {
            await this.LoadFromQueryAsync(StorageTable.CombineFilters(StorageTable.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey), "and", StorageTable.GenerateDateFieldFilter(dateFieldName, from, to)));
        }

        public async Task LoadByPartitionKeyAndTimeStampAsync(
            string partitionKey,
            DateTime from,
            DateTime to)
        {
            await this.LoadFromQueryAsync(StorageTable.CombineFilters(StorageTable.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey), "and", StorageTable.GenerateTimestampFilter(from, to)));
        }

        public async Task SaveToTable()
        {
            CustomTableEntityList customTableEntityList = this;
            customTableEntityList._tbl.StartBatchOperation();
            foreach (CustomTableEntity customTableEntity in (List<CustomTableEntity>)customTableEntityList)
            {
                if (customTableEntity != null)
                {
                    customTableEntity._st = customTableEntityList._tbl;
                    await customTableEntity.SaveToTable_Upsert();
                }
            }
            customTableEntityList._tbl.CompleteBatchOperation();
        }


        static public string GetFullyQualifiedBlobName(string tableName, string blobName)
        {
            return (tableName + "/" + blobName).ToLower() ?? "";
        }

        public string GetFullyQualifiedBlobName()
        {
            return GetFullyQualifiedBlobName(this.Table._tableName, GetBlobName()).ToLower();
        }

        public virtual string GetBlobName() => "";

        public string BlobContainerName => this.GetType().Name.ToLower() ?? "";

        public string BlobStorageAccountConnectionString => this.Table.ConnectionString;

        public async Task DeleteBlobByName(string BlobName)
        {
            await new BlobFunctions(this.BlobStorageAccountConnectionString).DeleteBlob(this.BlobContainerName, BlobName);
        }

        public async Task SaveToBlob(string blobName = "")
        {
            CustomTableEntityList customTableEntityList = this;
            BlobFunctions blobFunctions = new BlobFunctions(customTableEntityList.BlobStorageAccountConnectionString);
            string str = JsonConvert.SerializeObject((object)customTableEntityList);
            string blobContainerName = customTableEntityList.BlobContainerName;
            if (blobName == "")
            {
                blobName = customTableEntityList.GetFullyQualifiedBlobName();
            }
            await blobFunctions.Save(blobContainerName, blobName, str);
        }

        public async Task<bool> LoadFromBlob(string blobName)
        {
            CustomTableEntityList customTableEntityList = this;
            BlobFunctions blob = new BlobFunctions(customTableEntityList.BlobStorageAccountConnectionString);
            string fullBlobName = blobName;
            if (!await blob.Exists(customTableEntityList.BlobContainerName, fullBlobName))
                return false;
            TableEntityList tableEntityList = JsonConvert.DeserializeObject<TableEntityList>(await blob.Load(customTableEntityList.BlobContainerName, fullBlobName), new JsonSerializerSettings()
            {
                DateTimeZoneHandling = (DateTimeZoneHandling)0
            });
            // ISSUE: explicit non-virtual call
            customTableEntityList.Clear();
            foreach (TableEntity end in (List<TableEntity>)tableEntityList)
            {
                CustomTableEntity entity = customTableEntityList.GetEntity(end);
                // ISSUE: explicit non-virtual call
                customTableEntityList.Add(entity);
            }
            return true;
        }

        #region Paged
        LinkedList<CustomTableEntityList> _pageList = new LinkedList<CustomTableEntityList>();
        LinkedListNode<CustomTableEntityList> _pageCurrNode;
        string _pageFilter;
        int _pageSize;
        string _pageContinuationToken;

        public bool _moreData { get { return !string.IsNullOrEmpty(_pageContinuationToken); } }

        public bool PageFirstItem
        {
            get
            {
                if (_pageList != null)
                {
                    return _pageCurrNode == _pageList.First;
                }
                return false;
            }
        }

        public bool PageLastItem
        {
            get
            {
                if (_pageList != null)
                {
                    return _pageCurrNode == _pageList.Last;
                }
                return true;
            }
        }

        public bool LoadPreviouslyPagedData()
        {
            if (_pageCurrNode != null)
            {
                if (_pageCurrNode.Previous != null)
                {
                    _pageCurrNode = _pageCurrNode.Previous;
                    CustomTableEntityList lst = _pageCurrNode.Value;

                    Clear();
                    foreach (CustomTableEntity item in lst)
                    {
                        Add(item);
                    }
                    return true;
                }
            }
            return false;
        }

        private bool LoadNextPagedData()
        {
            if (_pageCurrNode != null)
            {
                if (_pageCurrNode.Next != null)
                {
                    _pageCurrNode = _pageCurrNode.Next;
                    CustomTableEntityList lst = _pageCurrNode.Value;

                    Clear();
                    foreach (CustomTableEntity item in lst)
                    {
                        Add(item);
                    }
                    return true;
                }
            }
            return false;
        }

        public async Task LoadFirstPagedQueryAsync(string finalFilter, int pageSize)
        {
            Clear();
            var page = await _tbl.GetPagedRowsByFilterAsync(finalFilter, pageSize, null); 
            _pageContinuationToken = page.Item1;
            foreach (TableEntity item in page.Item2)
            {
                Add(GetEntity(item));
            }

            CustomTableEntityList entry = DeepCopy((List<TableEntity>)page.Item2);
            _pageFilter = finalFilter;
            _pageSize = pageSize;
            _pageList.AddFirst(entry);
            _pageCurrNode = _pageList.First;
        }

        public async Task LoadNextPagedQueryAsync()
        {
            if (!LoadNextPagedData())
            {
                Clear();
                var page = await _tbl.GetPagedRowsByFilterAsync(_pageFilter, _pageSize, _pageContinuationToken);
                _pageContinuationToken = page.Item1;
                foreach (TableEntity item in page.Item2)
                {
                    Add(GetEntity(item));
                }

                CustomTableEntityList entry = DeepCopy((List<TableEntity>)page.Item2);
                _pageList.AddLast(entry);
                _pageCurrNode = _pageList.Last;
            }
        }
        #endregion
    }
}

using Azure;
using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

#nullable disable
namespace AzureDataImportLibrary
{
    public class StorageTable
    {
        private TableServiceClient _serviceClient;
        public TableClient _tableClient;
        protected string _connectionString;
        private bool _bTableCreated;

        public string _tableName { get; set; }

        public List<TableTransactionAction> _batchOperation { get; set; }

        public string ConnectionString => this._connectionString;

        public static string ValidStorageTableName(string input)
        {
            return new Regex("[^a-zA-Z0-9]").Replace(input, "");
        }

        public StorageTable()
        {
        }

        public StorageTable(string storagekey)
        {
            this._serviceClient = new TableServiceClient(storagekey);
            this._connectionString = storagekey;
        }

        public StorageTable(string storagekey, string TableName, bool bCreateIfNotExists = false)
        {
            this._serviceClient = new TableServiceClient(storagekey);
            this._connectionString = storagekey;
            this.Init(TableName, bCreateIfNotExists);
        }

        protected void Init(string TableName, bool bCreateIfNotExists = false)
        {
            this._tableName = TableName;
            this._batchOperation = (List<TableTransactionAction>)null;
            this._tableClient = new TableClient(this._connectionString, this._tableName);
            if (!bCreateIfNotExists)
                return;
            this.CreateIfNotExists();
        }

        public void CreateIfNotExists()
        {
            if (this._bTableCreated)
                return;
            int num = 0;
            bool flag = false;
            while (num++ < 3)
            {
                try
                {
                    this._serviceClient.CreateTableIfNotExists(this._tableName, new CancellationToken());
                    flag = true;
                    break;
                }
                catch (Exception)
                {
                    Thread.Sleep(100);
                }
            }
            if (!flag)
                throw new Exception("Failed to create the table " + this._tableName);
            this._bTableCreated = true;
        }

        #region Batch Operations
        public void StartBatchOperation() => this._batchOperation = new List<TableTransactionAction>();

        public void CompleteBatchOperation()
        {
            if (this._batchOperation == null || this._batchOperation.Count == 0)
                return;
            this.PersistData();
            this._batchOperation = (List<TableTransactionAction>)null;
        }

        protected bool IsBatchOperation() => this._batchOperation != null;

        protected void Batch_Update(TableEntity entity)
        {
            this._batchOperation.Add(new TableTransactionAction(TableTransactionActionType.UpdateReplace, (ITableEntity)entity));
            if (this._batchOperation.Count < 100)
                return;
            this.PersistData();
        }

        protected void Batch_Upsert(TableEntity entity)
        {
            this._batchOperation.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, (ITableEntity)entity));
            if (this._batchOperation.Count < 100)
                return;
            this.PersistData();
        }

        protected void Batch_Delete(TableEntity entity)
        {
            this._batchOperation.Add(new TableTransactionAction(TableTransactionActionType.Delete, (ITableEntity)entity));
            if (this._batchOperation.Count < 100)
                return;
            this.PersistData();
        }

        protected void PersistData()
        {
            try
            {
                if (this._batchOperation.Count <= 0)
                    return;
                this._tableClient.SubmitTransaction((IEnumerable<TableTransactionAction>)this._batchOperation, new CancellationToken());
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                this._batchOperation.Clear();
                GC.Collect();
            }
        }
        #endregion

        #region Generate Filter Strings
        public static string GeneratePartitionKeyAndRowKeyFilter(string partitionKey, string rowKey)
        {
            return StorageTable.CombineFilters(StorageTable.GeneratePartitionKeyFilter(partitionKey), "and", StorageTable.GenerateRowKeyFilter(rowKey));
        }

        public static string GeneratePartitionKeyFilter(string partitionKey)
        {
            return TableClient.CreateQueryFilter<TableEntity>((Expression<Func<TableEntity, bool>>)(e => e.PartitionKey == partitionKey));
        }

        public static string GenerateRowKeyFilter(string rowKey)
        {
            return TableClient.CreateQueryFilter<TableEntity>((Expression<Func<TableEntity, bool>>)(e => e.RowKey == rowKey));
        }

        public static string GenerateTimestampFilter(DateTime utcFrom, DateTime utcTo)
        {
            string queryFilter1 = TableClient.CreateQueryFilter<TableEntity>((Expression<Func<TableEntity, bool>>)(e => e.Timestamp >= (DateTimeOffset?)(DateTimeOffset)utcFrom));
            string queryFilter2 = TableClient.CreateQueryFilter<TableEntity>((Expression<Func<TableEntity, bool>>)(e => e.Timestamp <= (DateTimeOffset?)(DateTimeOffset)utcTo));
            DefaultInterpolatedStringHandler interpolatedStringHandler = new DefaultInterpolatedStringHandler(9, 2);
            interpolatedStringHandler.AppendLiteral("(");
            interpolatedStringHandler.AppendFormatted(queryFilter1);
            interpolatedStringHandler.AppendLiteral(") and (");
            interpolatedStringHandler.AppendFormatted(queryFilter2);
            interpolatedStringHandler.AppendLiteral(")");
            return interpolatedStringHandler.ToStringAndClear();
        }

        public static string GenerateDateFieldFilter(
            string dateFieldName,
            DateTime utcFrom,
            DateTime utcTo)
        {
            return StorageTable.CombineFilters(StorageTable.GenerateFilterConditionForDate(dateFieldName, QueryComparisons.GreaterThanOrEqual, utcFrom), "and", StorageTable.GenerateFilterConditionForDate(dateFieldName, QueryComparisons.LessThanOrEqual, utcTo));
        }

        public static string GetOpString(QueryComparisons op)
        {
            switch (op)
            {
                case QueryComparisons.Equal:
                    return "eq";
                case QueryComparisons.GreaterThanOrEqual:
                    return "ge";
                case QueryComparisons.LessThan:
                    return "lt";
                case QueryComparisons.LessThanOrEqual:
                    return "le";
                case QueryComparisons.GreaterThan:
                    return "gt";
                case QueryComparisons.NotEqual:
                    return "ne";
                default:
                    DefaultInterpolatedStringHandler interpolatedStringHandler = new DefaultInterpolatedStringHandler(24, 1);
                    interpolatedStringHandler.AppendLiteral("Uknown QueryComparisons ");
                    interpolatedStringHandler.AppendFormatted<QueryComparisons>(op);
                    throw new Exception(interpolatedStringHandler.ToStringAndClear());
            }
        }

        public static string GenerateFilterCondition(
            string FieldName,
            QueryComparisons op,
            string FieldValue)
        {
            DefaultInterpolatedStringHandler interpolatedStringHandler = new DefaultInterpolatedStringHandler(4, 3);
            interpolatedStringHandler.AppendFormatted(FieldName);
            interpolatedStringHandler.AppendLiteral(" ");
            interpolatedStringHandler.AppendFormatted(StorageTable.GetOpString(op));
            interpolatedStringHandler.AppendLiteral(" '");
            interpolatedStringHandler.AppendFormatted(FieldValue);
            interpolatedStringHandler.AppendLiteral("'");
            return interpolatedStringHandler.ToStringAndClear();
        }

        public static string GenerateFilterConditionForInt(
            string FieldName,
            QueryComparisons op,
            int FieldValue,
            bool bNot = false)
        {
            DefaultInterpolatedStringHandler interpolatedStringHandler = new DefaultInterpolatedStringHandler(2, 3);
            interpolatedStringHandler.AppendFormatted(FieldName);
            interpolatedStringHandler.AppendLiteral(" ");
            interpolatedStringHandler.AppendFormatted(StorageTable.GetOpString(op));
            interpolatedStringHandler.AppendLiteral(" ");
            interpolatedStringHandler.AppendFormatted<int>(FieldValue);
            string filterConditionForInt = interpolatedStringHandler.ToStringAndClear();
            if (bNot)
                filterConditionForInt = " not(" + filterConditionForInt + ")";
            return filterConditionForInt;
        }

        public static string GenerateFilterConditionForBool(
            string FieldName,
            QueryComparisons op,
            bool FieldValue)
        {
            DefaultInterpolatedStringHandler interpolatedStringHandler = new DefaultInterpolatedStringHandler(2, 3);
            interpolatedStringHandler.AppendFormatted(FieldName);
            interpolatedStringHandler.AppendLiteral(" ");
            interpolatedStringHandler.AppendFormatted(StorageTable.GetOpString(op));
            interpolatedStringHandler.AppendLiteral(" ");
            interpolatedStringHandler.AppendFormatted(FieldValue.ToString().ToLower());
            return interpolatedStringHandler.ToStringAndClear();
        }

        public static string GenerateFilterConditionForDate(
            string FieldName,
            QueryComparisons op,
            DateTime FieldValue,
            bool bIncNulls = false)
        {
            DefaultInterpolatedStringHandler interpolatedStringHandler = new DefaultInterpolatedStringHandler(13, 3);
            interpolatedStringHandler.AppendFormatted(FieldName);
            interpolatedStringHandler.AppendLiteral(" ");
            interpolatedStringHandler.AppendFormatted(StorageTable.GetOpString(op));
            interpolatedStringHandler.AppendLiteral(" datetime'");
            interpolatedStringHandler.AppendFormatted(FieldValue.ToString("s"));
            interpolatedStringHandler.AppendLiteral("Z'");
            string conditionForDate = interpolatedStringHandler.ToStringAndClear();
            if (bIncNulls)
                conditionForDate = conditionForDate + " or not(" + FieldName + " gt 0)";
            return conditionForDate;
        }

        public static string CombineFilters(string filter1, string op, string filter2)
        {
            DefaultInterpolatedStringHandler interpolatedStringHandler = new DefaultInterpolatedStringHandler(4, 3);
            interpolatedStringHandler.AppendLiteral("(");
            interpolatedStringHandler.AppendFormatted(filter1);
            interpolatedStringHandler.AppendLiteral(" ");
            interpolatedStringHandler.AppendFormatted(op);
            interpolatedStringHandler.AppendLiteral(" ");
            interpolatedStringHandler.AppendFormatted(filter2);
            interpolatedStringHandler.AppendLiteral(")");
            return interpolatedStringHandler.ToStringAndClear();
        }
        #endregion

        public TableEntity GetTopRowByPartitionKey(string partitionKey)
        {
            return this._tableClient.Query<TableEntity>(StorageTable.GeneratePartitionKeyFilter(partitionKey), new int?(), (IEnumerable<string>)null, new CancellationToken()).FirstOrDefault<TableEntity>();
        }

        public TableEntity GetLastRowByPartitionKey(string partitionKey)
        {
            return this._tableClient.Query<TableEntity>(StorageTable.GeneratePartitionKeyFilter(partitionKey), new int?(), (IEnumerable<string>)null, new CancellationToken()).LastOrDefault<TableEntity>();
        }

        public TableEntity GetFirstRowByPartitionKey(string partitionKey)
        {
            return this._tableClient.Query<TableEntity>(StorageTable.GeneratePartitionKeyFilter(partitionKey), new int?(), (IEnumerable<string>)null, new CancellationToken()).FirstOrDefault<TableEntity>();
        }

        public TableEntity GetRowByPartitionKeyAndRowKey(string partitionKey, string rowKey)
        {
            try
            {
                return (TableEntity)this._tableClient.GetEntity<TableEntity>(partitionKey, rowKey, (IEnumerable<string>)null, new CancellationToken());
            }
            catch (Exception)
            {
                return null;
            }
        }

        public async Task<TableEntity> GetRowByRowKeyAsync(string rowKey)
        {
            TableEntityList rowsByFilterAsync = await this.GetRowsByFilterAsync(StorageTable.GenerateRowKeyFilter(rowKey));
            return rowsByFilterAsync.Count != 1 ? (TableEntity)null : rowsByFilterAsync[0];
        }

        public TableEntity GetRowByRowKey(string rowKey)
        {
            TableEntityList rowsByFilter = GetRowsByFilter(StorageTable.GenerateRowKeyFilter(rowKey));
            if (rowsByFilter.Count > 0)
                return rowsByFilter[0];
            return null;
        }

        public async Task<TableEntityList> GetRowsByPartitionKeyAsync(string partitionKey)
        {
            return await this.GetRowsByFilterAsync(StorageTable.GeneratePartitionKeyFilter(partitionKey));
        }

        public async Task<TableEntityList> GetRowsByFilterAsync(string filter)
        {
            int numRec = 0;

            TableEntityList data = new TableEntityList();
            AsyncPageable<TableEntity> asyncPageable = _tableClient.QueryAsync<TableEntity>(filter);
            IAsyncEnumerator<TableEntity> asyncEnumerator = asyncPageable.GetAsyncEnumerator();
            try
            {
                while (await asyncEnumerator.MoveNextAsync())
                {
                    TableEntity current = asyncEnumerator.Current;
                    data.Add(current);
                    ++numRec;
                }
            }
            finally
            {
                if (asyncEnumerator != null)
                    await ((IAsyncDisposable)asyncEnumerator).DisposeAsync();
            }
            return data;
        }

        public TableEntityList GetRowsByFilter(string filter)
        {
            int numRec = 0;

            TableEntityList data = new TableEntityList();
            Pageable<TableEntity> pageable = _tableClient.Query<TableEntity>(filter);
            foreach (TableEntity e in pageable)
            {
                data.Add(e);
                ++numRec;
            }
            return data;
        }

        public async Task<TableEntity> GetRowByPartitionKeyAndRowKeyAsync(string partitionKey, string rowKey)
        {
            string keyAndRowKeyFilter = StorageTable.GeneratePartitionKeyAndRowKeyFilter(partitionKey, rowKey);
            this._tableClient.QueryAsync<TableEntity>(keyAndRowKeyFilter, new int?(), (IEnumerable<string>)null, new CancellationToken());
            TableEntityList rowsByFilterAsync = await this.GetRowsByFilterAsync(keyAndRowKeyFilter);
            return rowsByFilterAsync.Count <= 0 ? (TableEntity)null : rowsByFilterAsync[0];
        }

        public async Task DeleteEntriesByPartition(string partitionKey)
        {
            TableEntityList lst = await GetRowsByPartitionKeyAsync(partitionKey);
            if (lst.Count > 0)
            {
                await DeleteEntriesByEntityList(lst);
            }
        }

        public async Task DeleteAllEntries()
        {
            TableEntityList lst = await this.GetRowsByFilterAsync("");
            if (lst.Count > 0)
            {
                await DeleteEntriesByEntityList(lst);
            }        }

        protected async Task DeleteEntriesByEntityList(TableEntityList lst)
        {
            int x = 0;
            string lastPartitionKey = "";
            StartBatchOperation();
            try
            {
                foreach (TableEntity entity in lst)
                {
                    string PartitionKey = entity.PartitionKey;
                    if (lastPartitionKey == "" || (PartitionKey == lastPartitionKey && x<100))
                    {
                        await this.DeleteRow(entity);

                        lastPartitionKey = PartitionKey;
                        x++;
                    }
                    else
                    {
                        CompleteBatchOperation();
                        StartBatchOperation();
                        await this.DeleteRow(entity);
                        x = 1;
                        lastPartitionKey = "";
                    }

                }
            }
            finally
            {
                CompleteBatchOperation();
            }
        }

        public async Task DeleteRow(TableEntity entity, bool bAsync = false)
        {
            if (this.IsBatchOperation())
                this.Batch_Delete(entity);
            else if (bAsync)
            {
                Response response = await this._tableClient.DeleteEntityAsync(entity.PartitionKey, entity.RowKey, new ETag(), new CancellationToken());
            }
            else
                this._tableClient.DeleteEntity(entity.PartitionKey, entity.RowKey, new ETag(), new CancellationToken());
        }

        public async Task DeleteRow(string partitionKey, string rowKey, bool bAsync = false)
        {
            if (this.IsBatchOperation())
                this.Batch_Delete(new TableEntity(partitionKey, rowKey));
            else if (bAsync)
            {
                Response response = await this._tableClient.DeleteEntityAsync(partitionKey, rowKey, new ETag(), new CancellationToken());
            }
            else
                this._tableClient.DeleteEntity(partitionKey, rowKey, new ETag(), new CancellationToken());
        }

        public async Task UpdateRow(TableEntity insertentity, bool bAsync = false)
        {
            if (this.IsBatchOperation())
                this.Batch_Upsert(insertentity);
            else if (bAsync)
            {
                Response response = await this._tableClient.UpdateEntityAsync<TableEntity>(insertentity, ETag.All, TableUpdateMode.Merge, new CancellationToken());
            }
            else
                this._tableClient.UpdateEntity<TableEntity>(insertentity, ETag.All, TableUpdateMode.Merge, new CancellationToken());
        }

        public async Task UpsertRow(TableEntity insertentity, bool bAsync = false)
        {
            if (this.IsBatchOperation())
                this.Batch_Upsert(insertentity);
            else if (bAsync)
            {
                Response response = await this._tableClient.UpsertEntityAsync<TableEntity>(insertentity, TableUpdateMode.Merge, new CancellationToken());
            }
            else
                this._tableClient.UpsertEntity<TableEntity>(insertentity, TableUpdateMode.Merge, new CancellationToken());
        }

        #region Paging

        /// <summary>
        // Get first page:
        // var page = await GetPagedRowsByFilterAsync(filter, null);
        //string continuationToken = page.Item1;
        //    foreach (var item in page.Item2) {}

        // Get next page:
        // page = await GetPagedRowsByFilterAsync(filter, continuationToken);
        // continuationToken = page.Item1;
        // foreach (var item in page.Item2) {}

        /// </summary>
        /// <param name="filter"></param>
        /// <param name="continuationToken"></param>
        /// <returns></returns>
        public async Task<Tuple<string, IEnumerable<TableEntity>>> GetPagedRowsByFilterAsync(string filter, int pageSize, string continuationToken)
        {
            IList<TableEntity> modelList = new List<TableEntity>();
            var data = _tableClient.QueryAsync<TableEntity>(filter: filter, maxPerPage: pageSize);

            await foreach (var page in data.AsPages(continuationToken))
            {
                return Tuple.Create<string, IEnumerable<TableEntity>>(page.ContinuationToken, page.Values);
            }
            return null;
        }


        #endregion
    }
}

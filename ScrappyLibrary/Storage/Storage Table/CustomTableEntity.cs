using Azure;
using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

#nullable disable
namespace AzureDataImportLibrary
{
    public class CustomTableEntity : ITableEntity
    {
        public StorageTable _st;
        protected List<string> ExcludeColumns = new List<string>();

        protected Dictionary<string, object> additionalProperties = new Dictionary<string, object>();
        public Dictionary<string, object> AdditionalProperties { get { return additionalProperties; } }

        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public ETag ETag { get; set; }

        public CustomTableEntity() => this.Init("", "");

        public CustomTableEntity(string storageKey, string tblName, bool bCreateIfNotExists = false)
        {
            this.Init("", "");
            this._st = new StorageTable(storageKey, tblName, bCreateIfNotExists);
        }

        public CustomTableEntity(StorageTable st)
        {
            this.Init("", "");
            this._st = st;
        }

        public CustomTableEntity(StorageTable st, TableEntity e)
        {
            this.Init(e.PartitionKey, e.RowKey);
            this._st = st;
            this.SetData(e);
        }

        protected void Init(string PartitionKey, string RowKey)
        {
            this.InitExcludedColumns();
            this.InitTableEntity(PartitionKey, RowKey);
        }

        protected virtual void InitExcludedColumns()
        {
            this.AddExcludedColumn("PartitionKey");
            this.AddExcludedColumn("RowKey");
            this.AddExcludedColumn("Timestamp");
            this.AddExcludedColumn("ETag");
            this.AddExcludedColumn("AdditionalProperties");
            this.AddExcludedColumn("odata.etag");
            
        }

        public virtual bool UseInHTMLExport()
        {
            return true;
        }

        protected virtual void InitTableEntity(string PartitionKey, string RowKey)
        {
            this.PartitionKey = PartitionKey;
            this.RowKey = RowKey;
        }

        protected void AddEntity(TableEntity entity, string propName, object propValue)
        {
            if (!this.ExcludeColumns.Contains(propName))
            {
                entity.Add(propName, propValue);
            }
        }

        public void AddAdditionalProperty(string propName, object value, string pre = "")
        {
            if (!string.IsNullOrEmpty(pre))
            {
                propName = pre + "_" + propName;
            }
            if (!additionalProperties.ContainsKey(propName))
            {
                additionalProperties.Add(propName, value);
            }
            else
            {
                Debug.WriteLine($"ERROR: {propName} already exists. Object: {GetType().ToString()}");
            }
        }

        public object GetAdditionalProperty(string fullpropName)
        {
            object val = null;
            if (additionalProperties.ContainsKey(fullpropName))
            {
                val = additionalProperties[fullpropName];
            }
            return val;
        }

        public string GetAdditionalPropertyString(string fullpropName)
        {
            object val = GetAdditionalProperty(fullpropName);

            string retStr = "";
            if (val != null)
            {
                retStr = val.ToString();
            }
            return retStr;
        }

        public Decimal GetAdditionalPropertyDecimal(string fullpropName, Decimal? defaultValue=null)
        {
            string val = GetAdditionalPropertyString(fullpropName);

            if (!string.IsNullOrEmpty(val))
            {
                return Decimal.Parse(val);
            }
            if (defaultValue.HasValue)
            {
                return defaultValue.Value;
            }
            throw new Exception($"Invalid Decimal Value. Additional Property {fullpropName}");
        }

        public void SetProperties(object data, string pre = "")
        {
            foreach (PropertyInfo property in data.GetType().GetProperties())
            {
                if (!this.ExcludeColumns.Contains(((MemberInfo)property).Name))
                {
                    if (property.GetValue(data) != null)
                    {
                        AddAdditionalProperty(property.Name, property.GetValue(data), pre);
                    }
                }
            }
            if (data.GetType() == typeof(CustomTableEntity))
            {
                CustomTableEntity entity = (CustomTableEntity)data;
                foreach (KeyValuePair<string, object> item in entity.AdditionalProperties)
                {
                    if (!this.ExcludeColumns.Contains(item.Key))
                    {
                        if (item.Value != null)
                        {
                            AddAdditionalProperty(item.Key, item.Value, pre);
                        }
                    }
                }
            }
        }

        protected void AddEntity(TableEntity entity, PropertyInfo property, object source)
        {
            if (!this.ExcludeColumns.Contains(((MemberInfo)property).Name))
            {
                Type nullableType = property.PropertyType;
                Type underlyingType = Nullable.GetUnderlyingType(nullableType);
                if (underlyingType != (Type)null)
                    nullableType = underlyingType;
                if (nullableType.BaseType == typeof(Enum))
                {
                    string str = property.GetValue(source).ToString();
                    entity.Add(((MemberInfo)property).Name, (object)str);
                }
                else if (nullableType == typeof(Decimal) || nullableType == typeof(double) || nullableType == typeof(string) || nullableType == typeof(long) || nullableType == typeof(int) || nullableType == typeof(bool) || nullableType == typeof(DateTime))
                {
                    object obj = property.GetValue(source);
                    entity.Add(((MemberInfo)property).Name, obj);
                }
            }
        }

        public TableEntity GetEntity()
        {
            TableEntity entity = new TableEntity(this.PartitionKey, this.RowKey);
            foreach (PropertyInfo property in this.GetType().GetProperties((BindingFlags)52))
            {
                AddEntity(entity, property, this);
            }
            foreach (KeyValuePair<string, object> entry in additionalProperties)
            {
                AddEntity(entity, entry.Key, entry.Value);
            }
            return entity;
        }

        public void AddExcludedColumn(string colName) => this.ExcludeColumns.Add(colName);

        private void SetValueTypeProperty(PropertyInfo ps, Type propertyType, string itemValue)
        {
            if (propertyType == typeof(Decimal))
            {
                if (!itemValue.Contains("E"))
                    ps.SetValue((object)this, (object)Decimal.Parse(itemValue));
                else
                    ps.SetValue((object)this, (object)Decimal.Parse(itemValue, NumberStyles.Float));
            }
            else if (propertyType == typeof(double))
                ps.SetValue((object)this, (object)double.Parse(itemValue));
            else if (propertyType == typeof(int))
                ps.SetValue((object)this, (object)int.Parse(itemValue));
            else if (propertyType == typeof(long))
                ps.SetValue((object)this, (object)long.Parse(itemValue));
            else if (propertyType == typeof(bool))
            {
                ps.SetValue((object)this, (object)bool.Parse(itemValue.ToLower()));
            }
            else
            {
                DateTime result;
                if (DateTime.TryParse(itemValue, out result))
                {
                    ps.SetValue((object)this, (object)new DateTime(result.ToUniversalTime().Ticks, DateTimeKind.Utc));
                }
                else
                {
                    DefaultInterpolatedStringHandler interpolatedStringHandler = new DefaultInterpolatedStringHandler(21, 1);
                    interpolatedStringHandler.AppendLiteral("Unknown PropertyType ");
                    interpolatedStringHandler.AppendFormatted<Type>(propertyType);
                    throw new Exception(interpolatedStringHandler.ToStringAndClear());
                }
            }
        }

        public void SetData(TableEntity e)
        {
            bool bDebug = false;
            this.PartitionKey = e.PartitionKey;
            this.RowKey = e.RowKey;
            this.Timestamp = e.Timestamp;
            this.ETag = e.ETag;
            foreach (KeyValuePair<string, object> keyValuePair in (IEnumerable<KeyValuePair<string, object>>)e)
            {
                if (bDebug) Debug.WriteLine($"==============================================");
                if (bDebug) Debug.WriteLine($"Key:{keyValuePair.Key}");
                if (!this.ExcludeColumns.Contains(keyValuePair.Key))
                {
                    if (bDebug) Debug.WriteLine($"Not excluded");
                    bool bFoundProperty = false;
                    foreach (PropertyInfo property in this.GetType().GetProperties((BindingFlags)52))
                    {
                        if (bDebug) Debug.WriteLine($"Key:{keyValuePair.Key}, Property:{property.Name}");
                        if (keyValuePair.Key == property.Name)
                        {
                            if (bDebug) Debug.WriteLine($"Handled");
                            bFoundProperty = true;
                            try
                            {

                                Type propertyType = property.PropertyType;
                                string itemValue = keyValuePair.Value.ToString();
                                if (property.CanWrite)
                                {
                                    if (property.PropertyType.BaseType == typeof(Enum))
                                        property.SetValue((object)this, Enum.Parse(property.PropertyType, itemValue));
                                    else if (property.PropertyType.BaseType == typeof(object))
                                        property.SetValue((object)this, keyValuePair.Value);
                                    else if (property.PropertyType.BaseType == typeof(ValueType))
                                    {
                                        Type underlyingType = Nullable.GetUnderlyingType(propertyType);
                                        if (underlyingType != (Type)null)
                                        {
                                            if (itemValue != null)
                                                this.SetValueTypeProperty(property, underlyingType, itemValue);
                                        }
                                        else
                                            this.SetValueTypeProperty(property, propertyType, itemValue);
                                    }
                                    else
                                    {
                                        DefaultInterpolatedStringHandler interpolatedStringHandler = new DefaultInterpolatedStringHandler(17, 1);
                                        interpolatedStringHandler.AppendLiteral("Unknown BaseType ");
                                        interpolatedStringHandler.AppendFormatted<Type>(propertyType.BaseType);
                                        throw new Exception(interpolatedStringHandler.ToStringAndClear());
                                    }
                                }
                            }
                            catch (Exception)
                            {
                                Debug.WriteLine($"ERROR: FAILED to add property {keyValuePair.Key} to the {GetType().ToString()} object.");
                            }
                        }
                    }

                    if (!bFoundProperty)
                    {
                        if (bDebug) Debug.WriteLine($"Property not found - add to additional properties");
                        AddAdditionalProperty(keyValuePair.Key, keyValuePair.Value);
                    }
                }
                else
                {
                    if (bDebug) Debug.WriteLine($"Excluded");
                }
            }
        }

        public async Task<bool> LoadFromTableAsync()
        {
            TableEntity rowAsync = await this._st.GetRowByPartitionKeyAndRowKeyAsync(this.PartitionKey, this.RowKey);
            if (rowAsync == null)
                return false;
            this.SetData(rowAsync);
            return true;
        }

        public bool GetByPartitionAndRowKey(string partitionKey, string rowKey)
        {
            try
            {
                TableEntity partitionKeyAndRowKey = this._st.GetRowByPartitionKeyAndRowKey(partitionKey, rowKey);
                if (partitionKeyAndRowKey != null)
                {
                    this.SetData(partitionKeyAndRowKey);
                    return true;
                }
            }
            catch (Exception)
            {
            }
            return false;
        }

        //public async Task<bool> GetByPartitionAndRowKeyAsync(string partitionKey, string rowKey)
        //{
        //    TableEntity rowAsync = await this._st.GetRowByPartitionKeyAndRowKeyAsync(partitionKey, rowKey);
        //    if (rowAsync == null)
        //        return false;
        //    this.SetData(rowAsync);
        //    return true;
        //}

        public async Task<bool> GetByPartitionAndRowKeyAsync(string partitionKey="", string rowKey = "")
        {
            if (rowKey == "")
            {
                rowKey = RowKey;
            }
            if (partitionKey == "")
            {
                partitionKey = PartitionKey;
            }

            TableEntity entity = await _st.GetRowByPartitionKeyAndRowKeyAsync(partitionKey, rowKey);
            if (entity == null)
                return false;
            this.SetData(entity);
            return true;
        }

        public async Task<bool> GetByRowKeyAsync(string rowKey = "")
        {
            if (rowKey == "")
            {
                rowKey = RowKey;
            }
            TableEntity entity = await this._st.GetRowByRowKeyAsync(rowKey);
            if (entity == null)
                return false;
            this.SetData(entity);
            return true;
        }

        public bool GetByRowKey(string rowKey = "")
        {
            if (rowKey == "")
            {
                rowKey = RowKey;
            }
            TableEntity rowByRowKeyAsync = _st.GetRowByRowKey(rowKey);
            if (rowByRowKeyAsync == null)
                return false;
            this.SetData(rowByRowKeyAsync);
            return true;
        }

        public bool GetByPartitionKeyAndRowKey(string partitionKey = "", string rowKey = "")
        {
            if (rowKey == "")
            {
                rowKey = RowKey;
            }
            if (partitionKey == "")
            {
                partitionKey = PartitionKey;
            }
            TableEntity rowByRowKeyAsync = _st.GetRowByPartitionKeyAndRowKey(partitionKey, rowKey);
            if (rowByRowKeyAsync == null)
                return false;
            this.SetData(rowByRowKeyAsync);
            return true;
        }

        public bool GetByFirstRowByPartitionKey(string partitionKey)
        {
            TableEntity ent = _st._tableClient.Query<TableEntity>(StorageTable.GeneratePartitionKeyFilter(partitionKey), new int?(), (IEnumerable<string>)null, new CancellationToken()).FirstOrDefault<TableEntity>();
            if (ent == null)
                return false;
            this.SetData(ent);
            return true;
        }

        public virtual async Task DeleteFromTable()
        {
            await this._st.DeleteRow(this.PartitionKey, this.RowKey, true);
        }

        public void SaveToTable_UpsertSync()
        {
            _st.UpsertRow(this.GetEntity()).Wait();
        }

        public virtual async Task SaveToTable_Upsert(bool bAsync = true)
        {
            await this._st.UpsertRow(this.GetEntity(), bAsync);
        }

        public virtual async Task SaveToTable_Update(bool bAsync=true)
        {
            await this._st.UpdateRow(this.GetEntity(), bAsync);
        }


        #region Lock Feature
        protected bool _bLocked { get; set; }
        protected DateTime? _LockTime { get; set; }

        protected void Lock(bool bLock, string Comment = "")
        {
            if (bLock)
            {
                _bLocked = true;
                _LockTime = DateTime.UtcNow;
                //if (_log != null) _log.LogInfo($"Locked - {RowKey} - {Comment}");
            }
            else
            {
                _bLocked = false;
                _LockTime = null;
                //if (_log != null) _log.LogInfo($"Unlocked - {RowKey} - {Comment}");
            }
        }

        protected void _Checkout(string rowKey, string checkOutComment, int numRetries = 50)
        {
            int numTries = 0;
            while (numTries < numRetries)
            {
                GetByRowKey(rowKey);
                numTries++;
                if (_bLocked)
                {
                    System.Threading.Thread.Sleep(100);
                }
                else
                {
                    Lock(true, checkOutComment);
                    SaveToTable_UpsertSync();
                    return;
                }
            }
            throw new Exception($"Failed to Checkout the JobResult for {rowKey}");
        }

        protected void _Checkout(string partitionKey, string rowKey, string checkOutComment, int numRetries = 50)
        {
            int numTries = 0;
            while (numTries < numRetries)
            {
                GetByPartitionKeyAndRowKey(partitionKey, rowKey);
                numTries++;
                if (_bLocked)
                {
                    System.Threading.Thread.Sleep(100);
                }
                else
                {
                    Lock(true, checkOutComment);
                    SaveToTable_UpsertSync();
                    return;
                }
            }
            throw new Exception($"Failed to Checkout the JobResult for {RowKey}");
        }

        protected virtual void Checkout(string checkOutComment)
        {
            Checkout(RowKey, checkOutComment);
        }

        public virtual void Checkout(string rowKey, string checkOutComment)
        {
            //// Needs to be implemented in the derived classes like this

            //// Class Memeber variable
            //static readonly object _jobResultLock = new();

            //// Content of Checkout function
            //lock (_jobResultLock)
            //{
            //    _Checkout(ID, checkOutComment);
            //}
        }

        public virtual void Checkout(string partitionKey, string rowKey, string checkOutComment)
        {
            //// Needs to be implemented in the derived classes like this

            //// Class Memeber variable
            //static readonly object _jobResultLock = new();

            //// Content of Checkout function
            //lock (_jobResultLock)
            //{
            //    _Checkout(ID, checkOutComment);
            //}
        }

        public async Task Checkin(string checkInComment)
        {
            if (_bLocked)
            {
                try
                {
                    await SaveToTable_Update(true, checkInComment);
                }
                catch (Exception)
                {

                }
            }
        }

        public virtual async Task SaveToTable_Update(bool bUnlock, string Comment, bool bAsync=true)
        {
            DateTime dtNow = DateTime.UtcNow;
            if (!_bLocked)
            {
                throw new Exception("Item not locked. Cannot be updated.");
            }
            if (bUnlock)
            {
                Lock(false, Comment);
            }
            await SaveToTable_Update(bAsync);
            //if (_log != null) _log.LogInfo($"Successful Save/Checkin from {Comment} after {DateTime.UtcNow.Subtract(dtNow).TotalMilliseconds} ms", nameof(SaveToTable_Update));

        }

        public virtual async Task SaveToTable_Upsert(bool bUnlock, string Comment, bool bAsync, bool bVerifyIsLocked=true)
        {
            DateTime dtNow = DateTime.UtcNow;
            if (bVerifyIsLocked)
            {
                if (!_bLocked)
                {
                    throw new Exception("Item not locked. Cannot be upserted.");
                }
            }
            if (bUnlock)
            {
                Lock(false, Comment);
            }
            await SaveToTable_Upsert(bAsync);
            //if (_log != null) _log.LogInfo($"Successful Save/Checkin from {Comment} after {DateTime.UtcNow.Subtract(dtNow).TotalMilliseconds} ms", nameof(SaveToTable_Upsert));
        }

        #endregion

    }
}

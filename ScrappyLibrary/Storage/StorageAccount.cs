using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.Resources;
using Azure.ResourceManager.Resources.Models;
using Azure.ResourceManager.Storage;
using Azure.ResourceManager.Storage.Models;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

#nullable disable
namespace AzureDataImportLibrary
{
    public class StorageAccountClass
    {
        private string _rgName;
        private string _tenantID;
        private string _subscriptionID;
        private string _saName;
        private AzureLocation location = AzureLocation.EastUS;
        private StorageAccountCollection _accountCollection;
        private StorageAccountResource _storageAccount;

        public StorageAccountClass(string subscriptionID, string tenantID, string rgName)
        {
            this._rgName = rgName;
            this._subscriptionID = subscriptionID;
            this._tenantID = tenantID;
        }

        public string AccountName => this._saName;

        public string ConnectionString
        {
            get
            {
                if (this._storageAccount == null)
                    throw new Exception("The storageaccount has not been initialized.");
                using (IEnumerator<StorageAccountKey> enumerator = this._storageAccount.GetKeys(new StorageListKeyExpand?(), new CancellationToken()).GetEnumerator())
                {
                    if (enumerator.MoveNext())
                    {
                        StorageAccountKey current = enumerator.Current;
                        DefaultInterpolatedStringHandler interpolatedStringHandler = new DefaultInterpolatedStringHandler(87, 2);
                        interpolatedStringHandler.AppendLiteral("DefaultEndpointsProtocol=https;AccountName=");
                        interpolatedStringHandler.AppendFormatted(this._saName);
                        interpolatedStringHandler.AppendLiteral(";AccountKey=");
                        interpolatedStringHandler.AppendFormatted(current.Value);
                        interpolatedStringHandler.AppendLiteral(";EndpointSuffix=core.windows.net");
                        return interpolatedStringHandler.ToStringAndClear();
                    }
                }
                return "";
            }
        }

        protected async Task Init()
        {
            SubscriptionResource subscription = (SubscriptionResource)await new ArmClient((TokenCredential)new DefaultAzureCredential(new DefaultAzureCredentialOptions()
            {
                TenantId = this._tenantID
            })).GetSubscriptions().GetAsync(this._subscriptionID, new CancellationToken());
            ((ResourceProviderResource)await subscription.GetResourceProviderAsync("Microsoft.Storage", (string)null, new CancellationToken())).Register((ProviderRegistrationContent)null, new CancellationToken());
            this._accountCollection = (await subscription.GetResourceGroups().CreateOrUpdateAsync(WaitUntil.Completed, this._rgName, new ResourceGroupData(this.location), new CancellationToken())).Value.GetStorageAccounts();
            subscription = (SubscriptionResource)null;
        }

        protected StorageAccountCreateOrUpdateContent GetStorageAccountParameters(AzureLocation location)
        {
            return new StorageAccountCreateOrUpdateContent(new StorageSku(StorageSkuName.StandardLrs), StorageKind.Storage, location)
            {
                AllowSharedKeyAccess = new bool?(true)
            };
        }

        public async Task Create(string storAccountName)
        {
            await this.Init();
            this._saName = storAccountName;
            if (this._accountCollection == null)
                throw new Exception("The object has not been initialized.");
            this._storageAccount = (await this._accountCollection.CreateOrUpdateAsync(WaitUntil.Completed, storAccountName, this.GetStorageAccountParameters(this.location), new CancellationToken())).Value;
        }

        public async Task Delete()
        {
            if (this._storageAccount == null)
                throw new Exception("The storageaccount has not been initialized.");
            ArmOperation armOperation = await this._storageAccount.DeleteAsync(WaitUntil.Completed, new CancellationToken());
        }
    }
}

using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Threading.Tasks;

#nullable disable
namespace AzureDataImportLibrary
{
    public class BlobFunctions
    {
        private BlobServiceClient _blobServiceClient;

        public BlobFunctions(string ConnectionString)
        {
            this._blobServiceClient = new BlobServiceClient(ConnectionString);
        }

        public async Task<BlobContainerClient> GetContainer(string ContainerName)
        {
            BlobContainerClient container = this._blobServiceClient.GetBlobContainerClient(ContainerName);
            if (!(bool)await container.ExistsAsync())
            {
                Response<BlobContainerInfo> async = await container.CreateAsync();
            }
            return container;
        }

        public async Task DeleteBlob(string ContainerName, string BlobName)
        {
            Response<bool> response = await (await this.GetContainer(ContainerName)).GetBlobClient(BlobName).DeleteIfExistsAsync();
        }

        public async Task DeleteContainer(string ContainerName)
        {
            Response<bool> response = await this._blobServiceClient.GetBlobContainerClient(ContainerName).DeleteIfExistsAsync();
        }

        public async Task<int> DeleteSubContainersWithPrefix(string ContainerName, string Prefix)
        {
            var containerClient = _blobServiceClient.GetBlobContainerClient(ContainerName);

            // Use a prefix to list blobs in the "folder"
            var blobs = containerClient.GetBlobsByHierarchy(prefix: Prefix, delimiter: null);

            int num = 0;
            foreach (BlobHierarchyItem blob in blobs)
            {
                var blobClient = containerClient.GetBlobClient(blob.Blob.Name);
                /*await*/ blobClient.DeleteAsync(DeleteSnapshotsOption.IncludeSnapshots); // Delete the blob and its snapshots
                Debug.WriteLine($"Deleted Blob Container: {blob.Blob.Name}");
                num++;
            }
            return num;
        }

        public async Task<bool> Exists(string ContainerName, string BlobName)
        {
            return (bool)await (await this.GetContainer(ContainerName)).GetBlobClient(BlobName).ExistsAsync();
        }

        public async Task<string> Load(string ContainerName, string BlobName)
        {
            return ((BlobDownloadResult)await (await this.GetContainer(ContainerName)).GetBlobClient(BlobName).DownloadContentAsync()).Content.ToString();
        }

        public async Task Save(string ContainerName, string BlobName, string Data)
        {
            Response<BlobContentInfo> response = await (await this.GetContainer(ContainerName)).GetBlobClient(BlobName).UploadAsync(new BinaryData(Data));
        }
    }
}

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzureBlobTransfer
{
    public class BlobTransferManager
    {
        public BlobTransferManager()
        {
            MaxParallelDownloads = 8;
        }

        public int MaxParallelDownloads { get; set; }

        public async Task CopyContainer(CloudStorageAccount sourceAccount, CloudStorageAccount destinationAccount, string sourceContainerName, string destinationContainerName)
        {
            Task<CloudBlobContainer> srcContainerTask = GetContainer(sourceAccount, sourceContainerName), 
                                     destContainerTask = GetContainer(destinationAccount, destinationContainerName);

            await Task.WhenAll(srcContainerTask, destContainerTask);
            await CopyContainer(srcContainerTask.Result, destContainerTask.Result);
        }
        public async Task CopyContainer(CloudBlobContainer srcContainer, CloudBlobContainer destContainer)
        {
            BlobContinuationToken continuationToken = null;
            do
            {
                var segments = await srcContainer.ListBlobsSegmentedAsync(prefix: null, useFlatBlobListing: true,
                                                blobListingDetails: BlobListingDetails.Metadata, maxResults: MaxParallelDownloads,
                                                currentToken: continuationToken, options: null, operationContext: null);
                var tasks = new BlockingCollection<Task>(MaxParallelDownloads);

                Parallel.ForEach(segments.Results.Cast<CloudBlockBlob>(), srcFile =>
                {

                    var destLocation = srcFile.Uri.AbsoluteUri.Replace(srcContainer.Name, destContainer.Name);
                    var destFile = destContainer.GetBlockBlobReference(destLocation);
                    var copyTask = destFile.StartCopyFromBlobAsync(srcFile);

                    tasks.Add(copyTask);
                });

                await Task.WhenAll(tasks);
                continuationToken = segments.ContinuationToken;
            } while (continuationToken != null);
        }

        public async Task CopyContainer(CloudStorageAccount sourceAccount, string destination, string containerName)
        {
            CloudBlobContainer srcContainer = await GetContainer(sourceAccount, containerName);
            await CopyContainer(srcContainer, destination);
        }

        public async Task CopyContainer(CloudBlobContainer sourceContainer, string destination)
        {
            var uri = new Uri(sourceContainer.Uri.AbsoluteUri.TrimEnd('/') + '/');
            destination = Path.Combine(destination, sourceContainer.Name);

            BlobContinuationToken continuationToken = null;
            do
            {
                var segments = await sourceContainer.ListBlobsSegmentedAsync(prefix: null, useFlatBlobListing: true,
                                                blobListingDetails: BlobListingDetails.Metadata, maxResults: MaxParallelDownloads,
                                                currentToken: continuationToken, options: null, operationContext: null);
                
                var tasks = new BlockingCollection<Task>(MaxParallelDownloads);

                Parallel.ForEach(segments.Results.Cast<CloudBlockBlob>(), srcFile =>
                {
                    var relativePath = uri.MakeRelativeUri(srcFile.Uri);
                    var destLocation = Path.Combine(destination, relativePath.OriginalString);
                    
                    if (File.Exists(destLocation) && File.GetLastWriteTimeUtc(destLocation) == srcFile.Properties.LastModified)
                    {
                        // If the file looks unchanged, skip it.
                        return;
                    }
                    Directory.CreateDirectory(Path.GetDirectoryName(destLocation));
                    tasks.Add(srcFile.DownloadToFileAsync(destLocation, FileMode.Create));
                });

                await Task.WhenAll(tasks);
                continuationToken = segments.ContinuationToken;
            } while (continuationToken != null);
        }

        private static async Task<CloudBlobContainer> GetContainer(CloudStorageAccount storageAccount, string containerName)
        {
            var container = storageAccount.CreateCloudBlobClient()
                                          .GetContainerReference(containerName);
            await container.CreateIfNotExistsAsync();
            return container;
        }
    }
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Xunit;

namespace AzureBlobTransfer.Tests
{
    public class BlobTransferTest
    {
        private const string AccountName = "";
        private const string AccountKey = "";

        [Fact]
        public async Task CopyBlobCopiesSourceContainerToDestContainer()
        {
            VerifyKeys();

            // Arrange
            var account = new CloudStorageAccount(new StorageCredentials(AccountName, AccountKey), useHttps: true);
            var transferManager = new BlobTransferManager();
            var client = account.CreateCloudBlobClient();
            var srcContainer = client.GetContainerReference("feed-s25-049");
            var destContainer = client.GetContainerReference(Guid.NewGuid().ToString("N").ToLower());
            await destContainer.CreateAsync();

            try
            {
                // Act
                await transferManager.CopyContainer(srcContainer, destContainer);

                // Assert
                var srcItems = srcContainer.ListBlobs(prefix: null, useFlatBlobListing: true)
                                           .Select(s => s.Uri.LocalPath.TrimStart('/').Substring(srcContainer.Name.Length));
                var destItems = destContainer.ListBlobs(prefix: null, useFlatBlobListing: true)
                                             .Select(s => s.Uri.LocalPath.TrimStart('/').Substring(destContainer.Name.Length));
                Assert.True(new HashSet<string>(destItems, StringComparer.OrdinalIgnoreCase).SetEquals(srcItems));
            }
            finally
            {
                destContainer.DeleteIfExists();
            }
        }

        [Fact]
        public async Task CopyBlobCopiesSourceContainerToDestinationPath()
        {
            VerifyKeys();

            // Arrange
            var account = new CloudStorageAccount(new StorageCredentials(AccountName, AccountKey), useHttps: true);
            var transferManager = new BlobTransferManager();
            var client = account.CreateCloudBlobClient();
            var srcContainer = client.GetContainerReference("feed-s25-049");
            var destLocation = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

            try
            {
                // Act
                await transferManager.CopyContainer(srcContainer, destLocation);

                // Assert
                var srcItems = srcContainer.ListBlobs(prefix: null, useFlatBlobListing: true)
                                           .Select(s => s.Uri.LocalPath.TrimStart('/').Substring(srcContainer.Name.Length));
                destLocation = Path.Combine(destLocation, srcContainer.Name);
                var destItems = Directory.EnumerateFiles(destLocation, "*", SearchOption.AllDirectories)
                                         .Select(s => s.Substring(destLocation.Length).Replace('\\', '/'));
                Assert.True(new HashSet<string>(destItems, StringComparer.OrdinalIgnoreCase).SetEquals(srcItems));
            }
            finally
            {
                Directory.Delete(destLocation, recursive: true);
            }
        }

        private void VerifyKeys()
        {
            if (String.IsNullOrEmpty(AccountKey) || String.IsNullOrEmpty(AccountName))
            {
                throw new InvalidOperationException("Account Name or Account Key missing");
            }
        }
    }
}


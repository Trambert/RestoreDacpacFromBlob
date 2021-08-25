using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.SqlServer.Dac;

namespace RestoreBacpac
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static void Run([TimerTrigger("0 */55 * * * *")]TimerInfo myTimer, TraceWriter log)
        {
            log.Info($"C# Timer trigger function executed at: {DateTime.Now}");

            String sqlServerLogin = "XXXXX";
            String password = "XXXXX";
            String storageAccName = "XXX";
            String storageAccKey = "XXXX";
            String srvURL = "XXX.database.windows.net";
            String initialCatalog = "XXX";
            String containerName = "XXX";
            String bacpacFileName = "XXX.dacpac";
            String targetDBName = "XXXX";

            var dbCnxStr = @"Server=tcp:" + srvURL + ",1433;Initial Catalog=" + initialCatalog + ";Persist Security Info=False;User ID=" + sqlServerLogin + ";Password=" + password + ";MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";

            //Get dacpac from blob storage
            Microsoft.WindowsAzure.Storage.CloudStorageAccount account = new Microsoft.WindowsAzure.Storage.CloudStorageAccount(new StorageCredentials(storageAccName, storageAccKey), true);
            var blobClient = account.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(bacpacFileName);

            DacImportOptions options = new DacImportOptions();
            DacAzureDatabaseSpecification DBSpec = new DacAzureDatabaseSpecification
            {
                Edition = DacAzureEdition.Basic,
                ServiceObjective = "S3"
            };

            options.DatabaseSpecification = DBSpec;

            using (System.IO.MemoryStream ms = new System.IO.MemoryStream())
            {
                DacServices dac = new DacServices(dbCnxStr);
                dac.ProgressChanged += ServiceOnProgressChanged;
                dac.ImportBacpac(BacPackage.Load(ms),
                    targetDBName,
                    options);

            }
        }

        private static void ServiceOnProgressChanged(object sender, DacProgressEventArgs dacProgressEventArgs)
        {
            Console.WriteLine("{0} {1}", dacProgressEventArgs.Message, dacProgressEventArgs.Status);
        }
    }
}

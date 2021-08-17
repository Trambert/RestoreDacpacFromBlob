using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.SqlServer.Dac;
using Microsoft.WindowsAzure.Storage.Auth;

namespace TestFunctions
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static void Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, TraceWriter log)
        {
            String sqlServerLogin = "XXXXX";
            String password = "XXXXX";
            String storageAccName = "XXX";
            String storageAccKey = "XXXX";
            String srvURL = "XXX.database.windows.net";
            String initialCatalog = "XXX";
            String containerName = "XXX";
            String dacpacFileName = "XXX.dacpac";
            String targetDBName = "XXXX";

            var dbCnxStr = @"Server=tcp:"+srvURL+",1433;Initial Catalog="+initialCatalog+";Persist Security Info=False;User ID="+sqlServerLogin+";Password="+password+";MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";

            //Get dacpac from blob storage
            Microsoft.WindowsAzure.Storage.CloudStorageAccount account = new Microsoft.WindowsAzure.Storage.CloudStorageAccount(new StorageCredentials(storageAccName, storageAccKey), true);
            var blobClient = account.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(dacpacFileName);

            //Define publish option, database target type and objectives, incompatibility allowance
            var publishOptions = new PublishOptions();

            var dacOptions = new DacDeployOptions();
            dacOptions.CreateNewDatabase = true;
            dacOptions.ScriptDatabaseCompatibility = false;
            dacOptions.AllowIncompatiblePlatform = true;
            dacOptions.ExcludeObjectTypes = new ObjectType[1] { ObjectType.Assemblies };
            DacAzureDatabaseSpecification DBSpec = new DacAzureDatabaseSpecification();
            DBSpec.Edition = DacAzureEdition.Basic;
            DBSpec.ServiceObjective = "basic";
            dacOptions.DatabaseSpecification = DBSpec;
            publishOptions.DeployOptions = dacOptions;


            using (System.IO.MemoryStream ms = new MemoryStream())
            {
                blob.DownloadToStream(ms);
                var svc = new DacServices(dbCnxStr);
                svc.Publish(
                DacPackage.Load(ms),
                targetDBName,
                publishOptions
                );
            }
            Console.WriteLine("deploy finished");
        }
    }
}
